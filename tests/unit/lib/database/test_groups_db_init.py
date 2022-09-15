from dataclasses import dataclass, field
from typing import Any, Iterable
from src.lib.database import groups_db_init
import pytest


@dataclass
class CursorMock:
    execute_res: list[tuple[str, Any]] = field(default_factory=list)
    execute_many_res: list[tuple[str, Iterable[Any]]] = field(default_factory=list)

    def execute(self, __sql: str, __parameters: Any = ...) -> groups_db_init.Cursor:
        self.execute_res.append((__sql, __parameters))
        return self

    def executemany(self, __sql: str, __seq_of_parameters: Iterable[Any]) -> groups_db_init.Cursor:
        self.execute_many_res.append((__sql, __seq_of_parameters))
        return self


@pytest.fixture
def cursor() -> CursorMock:
    return CursorMock()


def test_create_db(cursor: CursorMock) -> None:
    groups_db_init.db_set_up(cursor)
    assert cursor.execute_many_res == []
    assert len(cursor.execute_res) == 1
    query, params = cursor.execute_res[0]
    assert params == ...
    expected_case_sensitive = [
        "groups",
        "group_id",
        "group_type",
        "name",
        "date",
        "items"
    ]
    for expected in expected_case_sensitive:
        assert expected in query
    expected_case_insensitive = [
        "CREATE TABLE IF NOT EXISTS groups",
        "group_id TEXT PRIMARY KEY",
        "group_type TEXT",
        "name TEXT",
        "date TEXT",
        "items TEXT"
    ]
    for expected in expected_case_insensitive:
        assert expected.lower() in query.lower()


def test_populate_table(cursor: CursorMock) -> None:
    data = [
        ("881bdb2b-ed5e-48bc-9c8c-4dcdd1d4d7d9", "msgroup", "some name", "1663268015.0499065", "a|b|c"),
        ("64a9dffa-1cef-4948-ba7b-eeffc1edc993", "msgroup", "other name", "1663268016.0499065", "a|b|d")
    ]
    groups_db_init.populate_table(cursor, data)
    assert cursor.execute_res == []
    assert len(cursor.execute_many_res) == 1
    query, params = cursor.execute_many_res[0]
    assert params == data
    assert "(?, ?, ?, ?, ?)" in query
    assert "groups" in query
