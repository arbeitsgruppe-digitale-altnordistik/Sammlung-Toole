from __future__ import annotations
from datetime import datetime, timezone

import sqlite3
from typing import Any, Protocol
from uuid import UUID

from src.lib.constants import DATABASE_GROUPS_PATH
from src.lib.groups import Group, GroupType
from src.lib import utils

log = utils.get_logger(__name__)


class Connection(Protocol):
    def cursor(self) -> Cursor:
        ...

    def commit(self) -> None:
        ...


class Cursor(Protocol):
    def execute(self, __sql: str, __parameters: Any = ...) -> Cursor:
        ...

    def fetchall(self) -> list[Any]:
        ...

    def close(self) -> None:
        ...


def create_connection(db_file: str = DATABASE_GROUPS_PATH) -> sqlite3.Connection:
    """create a database connection to the SQLite database specified by db_file

    Args:
        db_file (str, optional): database file

    Returns:
        sqlite3.Connection: Connection object or None
    """
    conn = sqlite3.connect(db_file)
    return conn


def get_ms_groups(cur: Cursor) -> list[Group]:
    return _query_groups(cur, 'SELECT * FROM groups WHERE group_type = "msgroup"')


def get_txt_groups(cur: Cursor) -> list[Group]:
    return _query_groups(cur, 'SELECT * FROM groups WHERE group_type = "txtgroup"')


def get_ppl_groups(cur: Cursor) -> list[Group]:
    return _query_groups(cur, 'SELECT * FROM groups WHERE group_type = "persgroup"')


def get_all_groups(cur: Cursor) -> list[Group]:
    return _query_groups(cur, 'SELECT * FROM groups')


def _get_group(t: tuple[str, str, str, str, str]) -> Group:
    id_s, type_s, name, date_s, items_s = t
    return Group(
        group_id=UUID(id_s),
        group_type=GroupType.from_string(type_s),
        name=name,
        date=datetime.fromtimestamp(float(date_s), timezone.utc).astimezone(),
        items=set(items_s.split('|')))


def _query_groups(cur: Cursor, query: str) -> list[Group]:
    cur.execute(query)
    res = cur.fetchall()
    groups = [_get_group(r) for r in res]
    return groups


def put_group(con: Connection, g: Group) -> None:
    cur = con.cursor()
    cur.execute(
        '''
        INSERT OR REPLACE INTO groups 
        VALUES(?, ?, ?, ?, ?)
        ''',
        (
            str(g.group_id),
            str(g.group_type.value),
            g.name,
            str(g.date.timestamp()),
            '|'.join(g.items)
        )
    )
    cur.close()
    con.commit()
    log.info(f"Saved group {g.name}")
