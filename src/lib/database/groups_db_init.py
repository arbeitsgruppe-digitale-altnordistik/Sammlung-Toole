
from __future__ import annotations

from logging import Logger
from typing import Any, Iterable, Protocol

import streamlit as st
from src.lib import utils


@st.experimental_singleton
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


class Connection(Protocol):
    def cursor(self) -> Cursor:
        ...

    def commit(self) -> None:
        ...


class Cursor(Protocol):
    def execute(self, __sql: str, __parameters: Any = ...) -> Cursor:
        ...

    def executemany(self, __sql: str, __seq_of_parameters: Iterable[Any]) -> Cursor:
        ...


def db_set_up(con: Connection) -> None:
    '''This function creates all the tables for the SQLite DB and defines the schema.

    Args:
        curse: a Cursor implementation

    Returns:
        None
    '''
    log.info("Setting up database tables...")
    cur = con.cursor()
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS groups (
            group_id TEXT PRIMARY KEY,
            group_type TEXT,
            name TEXT,
            date TEXT,
            items TEXT
        )
        '''
    )
    con.commit()
    log.info("Successfully created database tables.")
    # TODO: figure out how to solve items


def populate_table(con: Connection, incoming: list[tuple[str, str, str, str, str]]) -> None:
    '''This function will populate the 'groups' table with data.
    '''
    cur = con.cursor()
    cur.executemany('''INSERT OR IGNORE INTO groups VALUES (?, ?, ?, ?, ?)''', incoming)
    con.commit()
