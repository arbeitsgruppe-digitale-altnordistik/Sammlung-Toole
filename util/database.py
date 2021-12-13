from os import system
import sqlite3
from sqlite3 import Error
from typing import Any, Dict, List, Set, Tuple, Optional
from lxml import etree
import glob
import sys
from util.constants import *


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
    curse = conn.cursor()
    curse.execute('''CREATE TABLE IF NOT EXISTS people (firstName, lastName, persID primary key)''')
    return


def populate_people_table(conn: sqlite3.Connection, incoming: Any) -> None:
    curse = conn.cursor()
    curse.executemany('''INSERT INTO people VALUES (?, ?, ?)''', incoming)
    return


if __name__ == '__main__':
    conn = create_connection("./data/pythonsqlite.db")
    if not conn:
        sys.exit(1)
    cur = conn.cursor()
    for row in cur.execute('SELECT * FROM people ORDER BY persID'):
        print(row)
    conn.close()
