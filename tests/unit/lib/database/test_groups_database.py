from dataclasses import dataclass, field
from datetime import datetime
from dateutil.parser import parse
from typing import Any
from uuid import UUID

import pytest
from src.lib.database import groups_database
from src.lib.groups import Group, GroupType

query = "some string"
uuid = "a29f5597-d068-49c4-80f2-127024a06c28"
grouptype = "msgroup"
name = "some name"
timestamp = "1663273115.786848"
date = "2022-09-15 22:18:35.786848+02:00"
items = "a|b|c"


@dataclass
class CursorMock:
    execute_res: list[tuple[str, Any]] = field(default_factory=list)
    provided_fetch: list[tuple[str, str, str, str, str]] = field(default_factory=list)

    def execute(self, __sql: str, __parameters: Any = ...) -> groups_database.Cursor:
        self.execute_res.append((__sql, __parameters))
        return self

    def fetchall(self) -> list[Any]:
        return self.provided_fetch

    def close(self) -> None:
        ...


@dataclass
class ConnectionMock:
    c: CursorMock = field(default_factory=CursorMock)

    def cursor(self) -> groups_database.Cursor:
        return self.c

    def commit(self) -> None:
        ...


@pytest.fixture
def cursor() -> CursorMock:
    return CursorMock()


@pytest.fixture
def connection() -> ConnectionMock:
    return ConnectionMock()


def test_get_ms_groups(cursor: CursorMock) -> None:
    res = groups_database.get_ms_groups(cur=cursor)
    assert res == []
    assert len(cursor.execute_res) == 1
    query, params = cursor.execute_res[0]
    assert params == ...
    assert query == 'SELECT * FROM groups WHERE group_type = "msgroup"'


def test_get_txt_groups(cursor: CursorMock) -> None:
    res = groups_database.get_txt_groups(cur=cursor)
    assert res == []
    assert len(cursor.execute_res) == 1
    query, params = cursor.execute_res[0]
    assert params == ...
    assert query == 'SELECT * FROM groups WHERE group_type = "txtgroup"'


def test_get_ppl_groups(cursor: CursorMock) -> None:
    res = groups_database.get_ppl_groups(cur=cursor)
    assert res == []
    assert len(cursor.execute_res) == 1
    query, params = cursor.execute_res[0]
    assert params == ...
    assert query == 'SELECT * FROM groups WHERE group_type = "persgroup"'


def test__get_group() -> None:
    g = groups_database._get_group((uuid, grouptype, name, timestamp, items))
    assert g.group_id == UUID(uuid)
    assert g.group_type.value == grouptype
    assert g.name == name
    assert g.date == parse(date)
    assert g.items == {"b", "a", "c"}


def test__get_group_invalid() -> None:
    invalid_uuid = "a"
    invalid_grouptype = "mxgroup"
    invalid_timestamp_1 = date
    invalid_timestamp_2 = "-123456789123.456789"
    with pytest.raises(ValueError):
        groups_database._get_group((invalid_uuid, grouptype, name, timestamp, items))
    with pytest.raises(ValueError):
        groups_database._get_group((uuid, invalid_grouptype, name, timestamp, items))
    with pytest.raises(ValueError):
        groups_database._get_group((uuid, grouptype, name, invalid_timestamp_1, items))
    with pytest.raises(OSError):
        groups_database._get_group((uuid, grouptype, name, invalid_timestamp_2, items))


def test__query_groups(cursor: CursorMock) -> None:
    cursor.provided_fetch.append((uuid, grouptype, name, timestamp, items))
    res = groups_database._query_groups(cursor, query)
    assert len(res) == 1
    g = res[0]
    assert g.group_id == UUID(uuid)
    assert g.group_type.value == grouptype
    assert g.name == name
    assert g.date == parse(date)
    assert g.items == {"b", "a", "c"}
    assert len(cursor.execute_res) == 1
    q, params = cursor.execute_res[0]
    assert q == query
    assert params == ...


def test_put_group(connection: ConnectionMock) -> None:
    g = Group(
        group_type=GroupType.ManuscriptGroup,
        name=name,
        items={"a", "b", "c"},
        date=datetime.fromtimestamp(float(timestamp)),
        group_id=UUID(uuid)
    )
    groups_database.put_group(connection, g)
    assert len(connection.c.execute_res) == 1
    query, params = connection.c.execute_res[0]
    assert "(?, ?, ?, ?, ?)" in query
    assert "INSERT OR REPLACE INTO groups".lower() in query.lower()
    assert params[0] == uuid
    assert params[1] == grouptype
    assert params[2] == name
    assert params[3] == timestamp
    assert sorted(params[4].split('|')) == sorted(items.split('|'))
