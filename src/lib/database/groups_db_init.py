
from __future__ import annotations

from logging import Logger
from typing import Any, Iterable, Protocol

import streamlit as st
from src.lib import utils


@st.experimental_singleton   # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


class Cursor(Protocol):
    def execute(self, __sql: str, __parameters: Any = ...) -> Cursor:
        ...

    def executemany(self, __sql: str, __seq_of_parameters: Iterable[Any]) -> Cursor:
        ...

    def fetchall(self) -> list[Any]:
        ...

    def close(self) -> None:
        ...


def db_set_up(curse: Cursor) -> None:
    '''This function creates all the tables for the SQLite DB and defines the schema.

    Args:
        curse: a Cursor implementation

    Returns:
        None
    '''
    log.info("Setting up database tables...")
    curse.execute(
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
    log.info("Successfully created database tables.")
    # TODO: figure out how to solve items


def populate_table(curse: Cursor, incoming: list[tuple[str, str, str, str, str]]) -> None:
    '''This function will populate the 'groups' table with sample data.
    '''
    curse.executemany('''INSERT OR IGNORE INTO groups VALUES (?, ?, ?, ?, ?)''', incoming)
