from os import system
import sqlite3
from sqlite3 import Error
from typing import Any, Dict, List, Set, Tuple, Optional
from lxml import etree
import glob
import sys
from util.constants import *
import pandas as pd


nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}


def create_connection(db_file: str = DATABASE_PATH) -> Optional[sqlite3.Connection]:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

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
    curse.execute('''CREATE TABLE IF NOT EXISTS people (firstName, lastName, persID primary key)''')
    curse.execute('''CREATE TABLE IF NOT EXISTS manuscripts (shelfmark, shorttitle, country, settlement, repository, origin, date, terminusPostQuem, terminusAnteQuem, meandate, yearrange, support, folio, height, width, extent, description, creator, id, full_id primary key, filename)''')
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
    '''
    incoming2 = incoming[~incoming.duplicated(["full_id"])]
    dupl = incoming[incoming.duplicated(["full_id"])]
    print(dupl)
    incoming2.to_sql("manuscripts", conn, if_exists='append', index=False)
    return


def simple_search(conn: sqlite3.Connection, table_name: str, column_name: str, search_criteria: List[str]) -> pd.DataFrame:
    res = pd.DataFrame()
    first_run = True
    for i in search_criteria:
        ii = pd.read_sql(sql=f"SELECT * FROM {table_name} WHERE {column_name} = '{i}'", con=conn)
        if first_run:
            res = res.reindex(columns=ii.columns)
            first_run = False
        res = res.append(ii)
    res.reset_index(drop=True, inplace=True)
    return res


if __name__ == '__main__':
    conn = create_connection("./data/pythonsqlite.db")
    if not conn:
        sys.exit(1)
    cur = conn.cursor()
    for row in cur.execute('SELECT * FROM people ORDER BY persID'):
        print(row)
    conn.close()
