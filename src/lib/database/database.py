from __future__ import annotations

import sqlite3
from logging import Logger
from typing import Any, Protocol

import pandas as pd
import streamlit as st
from src.lib import utils
from src.lib.constants import DATABASE_PATH


@st.experimental_singleton   # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


class Cursor(Protocol):
    def execute(self, __sql: str, __parameters: Any = ...) -> Cursor:
        ...

    def fetchall(self) -> list[Any]:
        ...

    def close(self) -> None:
        ...


def create_connection(db_file: str = DATABASE_PATH) -> sqlite3.Connection:
    """create a database connection to the SQLite database specified by db_file

    Args:
        db_file(str): the database file

    Returns:
        Connection object or None
    """

    conn = sqlite3.connect(db_file)

    return conn


def get_metadata(conn: sqlite3.Connection, table_name: str, column_name: str, search_criteria: list[str]) -> pd.DataFrame:
    """One stop shop for simple search/SELECT queries.

    Args:
        conn(sqlite3.Connection): DB connection object
        table_name(str): Name of table to be queried
        column_name(str): Name of column on which to apply selection criteria
        search_criteria(List): What it is you are looking for

    Returns:
        pd.DataFrame
    """
    dfs: list[pd.DataFrame] = []
    for i in search_criteria:
        ii = pd.read_sql(sql=f"SELECT * FROM {table_name} WHERE {column_name} = '{i}'", con=conn)  # TODO: replace with ? notation
        dfs.append(ii)
    res = pd.concat(dfs)
    return res


def simple_people_search(cursor: Cursor, pers_id: str) -> str:
    cursor.execute(f"SELECT firstName, lastName FROM people WHERE persID = '{pers_id}'")  # TODO: replace with ? notation
    raw = cursor.fetchall()
    if len(raw) == 0:
        log.debug(f"No record in DB for: {pers_id}")
        return ""
    raw = raw[0]
    res = " ".join(raw)
    cursor.close()
    return res


def ms_x_ppl(cursor: Cursor, pers_ids: list[str]) -> list[str]:
    """
    Get IDs of all manuscripts related to a list of people.
    """
    query = f"SELECT msID FROM junctionPxM WHERE persID in ({', '.join('?' for _ in pers_ids)})"
    cursor.execute(query, pers_ids)
    res = {x[0] for x in cursor.fetchall()}
    return list(res)


def ppl_x_mss(cursor: Cursor, ms_ids: list[str]) -> list[str]:
    """
    Get IDs of all people connected to a list of manuscripts.
    Returns list of IDs for people.
    """
    query = f"SELECT persID FROM junctionPxM WHERE msID in ({', '.join('?' for _ in ms_ids)})"
    cursor.execute(query, ms_ids)
    res = {x[0] for x in cursor.fetchall()}
    return list(res)


def ms_x_txts(cursor: Cursor, txts: list[str]) -> list[str]:
    """
    Get IDs of all manuscripts connected to a list of texts.
    Returns list of IDs for manuscripts.
    """  # TODO: clarify text definition
    query = f"SELECT msID FROM junctionTxM WHERE txtName in ({', '.join('?' for _ in txts)})"
    cursor.execute(query, txts)
    res = {x[0] for x in cursor.fetchall()}
    return list(res)


def txts_x_ms(cursor: Cursor, ms_ids: list[str]) -> list[str]:
    """
    Get IDs of all texts connected to a list of manuscripts.
    Returns list of IDs for texts.
    """  # TODO: clarify text definition
    query = f"SELECT txtName FROM junctionTxM WHERE msID in ({', '.join('?' for _ in ms_ids)})"
    cursor.execute(query, ms_ids)
    res = {x[0] for x in cursor.fetchall()}
    return list(res)


def persons_lookup_dict(cursor: Cursor) -> dict[str, str]:
    """
    Gets the data from person(ID, first name, last name).
    Returns the lookup-dict for the IDs of people to their natural names.
    """
    cursor.execute('SELECT * FROM people')
    raw = cursor.fetchall()
    res = {x[0]: f"{x[1]} {x[2]}" for x in raw}
    return res


def ms_lookup_dict(cursor: Cursor) -> dict[str, list[str]]:
    """
    Returns the lookup-dict for the IDs of manuscripts to their human readable signatures.
    """
    cursor.execute('SELECT full_ID, shelfmark, shorttitle FROM manuscripts')
    raw = cursor.fetchall()
    res = {x[0]: [x[1], x[2]] for x in raw}
    return res


def txt_lookup_list(cursor: Cursor) -> list[str]:
    """
    Returns the lookup-list for the texts. Used in front end search.
    """
    cursor.execute('SELECT txtName FROM junctionTxM')
    raw = cursor.fetchall()
    res = list(set([x[0] for x in raw]))
    return res
