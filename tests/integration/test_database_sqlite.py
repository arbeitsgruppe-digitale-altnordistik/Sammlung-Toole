from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel

from src.lib.database.sqlite import database_sqlite_impl as database
from src.lib.database.sqlite.models import *


def test_create_sql_engine_inmemory() -> None:
    e = database.get_engine(':memory:')
    assert isinstance(e, Engine)
    SQLModel.metadata.create_all(e)
    with Session(e) as s:
        assert s.info == {}


# TODO: add more tests here
