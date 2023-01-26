import pytest
from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel

from src.lib.database.sqlite import database_sqlite_impl as database
from src.lib.database.sqlite.database_sqlite_impl import \
    DatabaseSQLiteImpl as Database
from src.lib.database.sqlite.models import *
from src.lib.groups import Group, GroupType


@pytest.fixture
def db() -> Database:
    engine = database.get_engine(':memory:')
    db = Database(engine)
    db.setup_db()
    return db


@pytest.fixture
def group1() -> Group:
    group = Group(
        GroupType.ManuscriptGroup,
        "group1",
        set()
    )
    return group


def test_create_sql_engine_inmemory() -> None:
    e = database.get_engine(':memory:')
    assert isinstance(e, Engine)
    SQLModel.metadata.create_all(e)
    with Session(e) as s:
        assert s.info == {}


def test_add_group(db: Database, group1: Group) -> None:
    db.add_group(group1)
    res = db.get_all_groups()
    assert len(res) == 1
    group_res = res.pop()
    assert group_res == group1

# TODO: add more tests here
