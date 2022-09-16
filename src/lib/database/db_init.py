import sqlite3
from logging import Logger
from typing import Any

import pandas as pd
import streamlit as st
from src.lib import utils


@st.experimental_singleton   # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


def db_set_up(conn: sqlite3.Connection) -> None:
    '''This function creates all the tables for the SQLite DB and
    defines the schema.

    Args:
        conn: SQLite Connection object

    Returns:
        None
    '''
    log.info("Setting up database tables...")
    curse = conn.cursor()
    curse.execute('''CREATE TABLE IF NOT EXISTS people (persID PRIMARY KEY
                                                        firstName,
                                                        lastName
                                                        )''')
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
    log.info("Successfully created database tables.")
    return


def populate_people_table(conn: sqlite3.Connection, incoming: list[tuple[str, str, str]]) -> None:
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


def populate_ms_table(conn: sqlite3.Connection, incoming: list[tuple[Any]]) -> None:
    '''Function to populate the manuscripts table with data.

    Args:
        conn(sqlite3.Connection): DB connection object
        incoming(pd.DataFrame): Dataframe containing the manuscript data. Column names
        of dataframe must match column names of db table.

    Returns:
        None
    '''
    curse = conn.cursor()
    sql_query = '''INSERT OR IGNORE INTO people VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    curse.executemany(sql_query, incoming)
    conn.commit()
    curse.close()


def populate_junctionPxM(conn: sqlite3.Connection, incoming: list[tuple[int, str, str]]) -> None:
    curse = conn.cursor()
    curse.executemany('''INSERT OR IGNORE INTO junctionPxM VALUES (?, ?, ?)''', incoming)
    conn.commit()
    curse.close()


def populate_junctionTxM(conn: sqlite3.Connection, incoming: list[tuple[str, str]]) -> None:
    curse = conn.cursor()
    curse.executemany("INSERT OR IGNORE INTO junctionTxM(msID, txtName) VALUES (?,?)", incoming)
    curse.close()
    return


def PxM_integrity_check(conn: sqlite3.Connection, incoming: list[tuple[int, str, str]]) -> bool:
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
