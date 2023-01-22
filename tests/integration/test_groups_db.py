from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel

from src.lib.database import groups_db


def test_create_sql_engine_inmemory() -> None:
    e = groups_db.get_engine(':memory:')
    assert isinstance(e, Engine)
    SQLModel.metadata.create_all(e)
    with Session(e) as s:
        assert s.info == {}
