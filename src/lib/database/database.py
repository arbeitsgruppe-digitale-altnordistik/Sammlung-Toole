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
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """

    conn = sqlite3.connect(db_file)

    return conn


# FIXME-BL: how to use my own cursor with pd.read_sql()?
# You use the conn object with pandas, it creates its own cursor /SK
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


def simple_people_search(curse: Cursor, persID: str) -> str:
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


def ms_x_ppl(curse: Cursor, pplIDs: list[str]) -> list[str]:
    """
    Get IDs of all manuscripts related to a list of people.
    """
    sqlQ = f"SELECT msID FROM junctionPxM WHERE persID in ({', '.join('?' for _ in pplIDs)})"
    curse.execute(sqlQ, pplIDs)
    res = {x[0] for x in curse.fetchall()}
    return list(res)


def ppl_x_mss(curse: Cursor, msIDs: list[str]) -> list[str]:
    """
    Get IDs of all people connected to a list of manuscripts.
    Returns list of IDs for people.
    """
    sqlQ = f"SELECT persID FROM junctionPxM WHERE msID in ({', '.join('?' for _ in msIDs)})"
    curse.execute(sqlQ, msIDs)
    res = {x[0] for x in curse.fetchall()}
    return list(res)


def ms_x_txts(curse: Cursor, txts: list[str]) -> list[str]:
    """
    Get IDs of all manuscripts connected to a list of texts.
    Returns list of IDs for manuscripts.
    """  # TODO: clarify text definition
    sqlQ = f"SELECT msID FROM junctionTxM WHERE txtName in ({', '.join('?' for _ in txts)})"
    curse.execute(sqlQ, txts)
    res = {x[0] for x in curse.fetchall()}
    return list(res)


def txts_x_ms(curse: Cursor, mss: list[str]) -> list[str]:
    """
    Get IDs of all texts connected to a list of manuscripts.
    Returns list of IDs for texts.
    """  # TODO: clarify text definition
    sqlQ = f"SELECT txtName FROM junctionTxM WHERE msID in ({', '.join('?' for _ in mss)})"
    curse.execute(sqlQ, mss)
    res = {x[0] for x in curse.fetchall()}
    return list(res)


def persons_lookup_dict(curse: Cursor) -> dict[str, str]:
    """
    Returns the lookup-dict for the IDs of people to their natural names.
    """
    curse.execute('SELECT * FROM people')
    raw = curse.fetchall()
    res = {x[2]: f"{x[0]} {x[1]}" for x in raw}
    return res


def ms_lookup_dict(curse: Cursor) -> dict[str, list[str]]:
    """
    Returns the lookup-dict for the IDs of manuscripts to their human readable signatures.
    """
    curse.execute('SELECT full_ID, shelfmark, shorttitle FROM manuscripts')
    raw = curse.fetchall()
    res = {x[0]: [x[1], x[2]] for x in raw}
    return res


def txt_lookup_list(curse: Cursor) -> list[str]:
    """
    Returns the lookup-list for the texts. Used in front end search.
    """
    curse.execute('SELECT txtName FROM junctionTxM')
    raw = curse.fetchall()
    res = list(set([x[0] for x in raw]))
    return res
