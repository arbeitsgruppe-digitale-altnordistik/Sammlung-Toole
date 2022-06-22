from os import system
import sqlite3
from sqlite3 import Error
from typing import Any, Dict, List, Set, Tuple, Optional, Union
from lxml import etree
import glob
import sys
from util.constants import *
import pandas as pd
from logging import Logger
from util import utils
import streamlit as st


nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}


@st.experimental_singleton   # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


def create_connection(db_file: str = DATABASE_PATH) -> sqlite3.Connection:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """

    conn = sqlite3.connect(db_file)

    return conn


def db_set_up(conn: sqlite3.Connection) -> None:
    '''This function creates all the tables for the SQLite DB and
    defines the schema.

    Args:
        conn: SQLite Connection object

    Returns:
        None
    '''
    curse = conn.cursor()
    curse.execute('''CREATE TABLE IF NOT EXISTS people (firstName,
                                                        lastName,
                                                        persID PRIMARY KEY)''')  # TODO: put primary keys in their rightful places
    curse.execute('''CREATE TABLE IF NOT EXISTS manuscripts (shelfmark,
                                                            shorttitle,
                                                            country,
                                                            settlement,
                                                            repository,
                                                            origin,
                                                            date,
                                                            terminusPostQuem,
                                                            terminusAnteQuem,
                                                            meandate,
                                                            yearrange,
                                                            support,
                                                            folio,
                                                            height,
                                                            width,
                                                            extent,
                                                            description,
                                                            creator,
                                                            id,
                                                            full_id PRIMARY KEY,
                                                            filename)''')
    curse.execute('''CREATE TABLE IF NOT EXISTS junctionPxM (locID integer PRIMARY KEY DEFAULT 0 NOT NULL,
                                                                persID,
                                                                msID,
                                                                FOREIGN KEY(persID) REFERENCES people(persID) ON DELETE CASCADE ON UPDATE CASCADE,
                                                                FOREIGN KEY(msID) REFERENCES manuscripts(full_id) ON DELETE CASCADE ON UPDATE CASCADE)''')
    curse.execute('''CREATE TABLE IF NOT EXISTS junctionTxM (locID integer PRIMARY KEY,
                                                                msID,
                                                                txtName,
                                                                FOREIGN KEY(msID) REFERENCES manuscripts(full_id) ON DELETE CASCADE ON UPDATE CASCADE)''')
    return


def populate_people_table(conn: sqlite3.Connection, incoming: List[Tuple[str, str, str]]) -> None:
    '''This function will populate the 'people' table with information about
    persons from the Handrit names authority file.
    Currently, the following information is held (in that order): First name, last name, unique handrit ID

    Args:
        conn (sqlite.Connection): DB connection object
        incoming (List[Tuple[str, str, str]]): First name, last name, and ID of persons to be stored
    '''
    curse = conn.cursor()
    curse.executemany('''INSERT OR IGNORE INTO people VALUES (?, ?, ?)''', incoming)
    return


def populate_ms_table(conn: sqlite3.Connection, incoming: pd.DataFrame) -> None:
    '''Function to populate the manuscripts table with data.

    Args:
        conn(sqlite3.Connection): DB connection object
        incoming(pd.DataFrame): Dataframe containing the manuscript data. Column names
        of dataframe must match column names of db table.

    Returns:
        None
    '''
    incoming2 = incoming[~incoming.duplicated(["full_id"])]
    dupl = incoming[incoming.duplicated(["full_id"])]
    incoming2.to_sql("manuscripts", conn, if_exists='append', index=False)
    return


def populate_junctionPxM(conn: sqlite3.Connection, incoming: List[Tuple[int, str, str]]) -> None:
    curse = conn.cursor()
    curse.executemany('''INSERT OR IGNORE INTO junctionPxM VALUES (?, ?, ?)''', incoming)
    curse.close()
    return


def populate_junctionTxM(conn: sqlite3.Connection, incoming: List[Tuple[str, str]]) -> None:
    curse = conn.cursor()
    curse.executemany("INSERT OR IGNORE INTO junctionTxM(msID, txtName) VALUES (?,?)", incoming)
    curse.close()
    return


def PxM_integrity_check(conn: sqlite3.Connection, incoming: List[Tuple[int, str, str]]) -> bool:
    curse = conn.cursor()
    curse.execute(f"SELECT persID FROM junctionPxM")
    l0 = [x[1] for x in incoming]
    l1 = []
    for row in curse.fetchall():
        l1.append(row[0])
    l2 = [x for x in l0 if x not in l1]
    l0.sort()
    l2.sort()
    print("Performing integrity check")
    check = l0 == l2
    return check


def get_metadata(conn: sqlite3.Connection, table_name: str, column_name: str, search_criteria: List[str]) -> pd.DataFrame:
    """One stop shop for simple search/SELECT queries.

    Args:
        conn(sqlite3.Connection): DB connection object
        table_name(str): Name of table to be queried
        column_name(str): Name of column on which to apply selection criteria
        search_criteria(List): What it is you are looking for

    Returns:
        pd.DataFrame
    """
    res = pd.DataFrame()
    first_run = True
    for i in search_criteria:
        ii = pd.read_sql(sql=f"SELECT * FROM {table_name} WHERE {column_name} = '{i}'", con=conn)  # TODO: replace with ? notation
        if first_run:
            res = res.reindex(columns=ii.columns)
            first_run = False
        res = res.append(ii)
    res.reset_index(drop=True, inplace=True)
    return res


def simple_people_search(conn: sqlite3.Connection, persID: str) -> str:
    curse = conn.cursor()
    curse.execute(f"SELECT firstName, lastName FROM people WHERE persID = '{persID}'")  # TODO: replace with ? notation
    raw = curse.fetchall()
    if len(raw) == 0:
        log.debug(f"No record in DB for: {persID}")
        res = ""
        return res
    raw = raw[0]
    res = " ".join(raw)
    curse.close()
    return res


def ms_x_ppl(conn: sqlite3.Connection, pplIDs: list[str]) -> list[str]:
    """
    Get IDs of all manuscripts related to a list of people.
    """
    curse = conn.cursor()
    sqlQ = f"SELECT msID FROM junctionPxM WHERE persID in ({', '.join('?' for _ in pplIDs)})"
    curse.execute(sqlQ, pplIDs)
    res = [x[0] for x in curse.fetchall()]
    return res


def ppl_x_mss(conn: sqlite3.Connection, msIDs: list[str]) -> list[str]:
    """
    Get IDs of all people connected to a list of manuscripts.
    Returns list of IDs for people.
    """
    curse = conn.cursor()
    sqlQ = f"SELECT persID FROM junctionPxM WHERE msID in ({', '.join('?' for _ in msIDs)})"
    curse.execute(sqlQ, msIDs)
    res = [x[0] for x in curse.fetchall()]
    return res


def ms_x_txts(conn: sqlite3.Connection, txts: list[str]) -> list[str]:
    """
    Get IDs of all manuscripts connected to a list of texts.
    Returns list of IDs for manuscripts.
    """  # TODO: clarify text definition
    curse = conn.cursor()
    sqlQ = f"SELECT msID FROM junctionTxM WHERE txtName in ({', '.join('?' for _ in txts)})"
    curse.execute(sqlQ, txts)
    res = [x[0] for x in curse.fetchall()]
    return res


def txts_x_ms(conn: sqlite3.Connection, mss: list[str]) -> list[str]:
    """
    Get IDs of all texts connected to a list of manuscripts.
    Returns list of IDs for texts.
    """  # TODO: clarify text definition
    curse = conn.cursor()
    sqlQ = f"SELECT txtName FROM junctionTxM WHERE msID in ({', '.join('?' for _ in mss)})"
    curse.execute(sqlQ, mss)
    res = [x[0] for x in curse.fetchall()]
    return res


def persons_lookup_dict(conn: sqlite3.Connection) -> dict[str, str]:
    """
    Returns the lookup-dict for the IDs of people to their natural names.
    """
    curse = conn.cursor()
    curse.execute('SELECT * FROM people')
    raw = curse.fetchall()
    res = {x[2]: f"{x[0]} {x[1]}" for x in raw}
    return res


def ms_lookup_dict(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """
    Returns the lookup-dict for the IDs of manuscripts to their human readable signatures.
    """
    curse = conn.cursor()
    curse.execute('SELECT full_ID, shelfmark, shorttitle FROM manuscripts')
    raw = curse.fetchall()
    res = {x[0]: [x[1], x[2]] for x in raw}
    return res


def txt_lookup_list(conn: sqlite3.Connection) -> list[str]:
    """
    Returns the lookup-list for the texts. Used in front end search.
    """
    curse = conn.cursor()
    curse.execute('SELECT txtName FROM junctionTxM')
    raw = curse.fetchall()
    res = list(set([x[0] for x in raw]))
    return res


if __name__ == '__main__':
    conn = create_connection("./data/pythonsqlite.db")
    if not conn:
        sys.exit(1)
    cur = conn.cursor()
    for row in cur.execute('SELECT * FROM people ORDER BY persID'):
        print(row)
    conn.close()
