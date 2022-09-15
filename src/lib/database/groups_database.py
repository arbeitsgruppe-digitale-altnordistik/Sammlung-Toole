from __future__ import annotations
from datetime import datetime

import sqlite3
from typing import Any, Protocol
from uuid import UUID

from src.lib.constants import DATABASE_GROUPS_PATH
from src.lib.groups import Group, GroupType


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


def get_all_groups(cur: Cursor) -> list[Group]:
    def get_group(t: tuple[str, str, str, str, str]) -> Group:
        id_s, type_s, name, date_s, items_s = t
        return Group(
            group_id=UUID(id_s),
            group_type=GroupType.from_string(type_s),
            name=name,
            date=datetime.fromtimestamp(float(date_s)),
            items=set(items_s.split('|')))

    cur.execute('SELECT * FROM groups')
    res = cur.fetchall()
    groups = [get_group(r) for r in res]
    return groups
