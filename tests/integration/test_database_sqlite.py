import pytest
from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel

from src.lib.database.sqlite import database_sqlite_impl as database
from src.lib.database.sqlite.database_sqlite_impl import \
    DatabaseSQLiteImpl as Database
from src.lib.groups import Group, GroupType
import dataclasses


@pytest.fixture
def db() -> Database:
    engine = database.get_engine(':memory:')
    db = Database(engine)
    db.setup_db()
    return db


@pytest.fixture
def group_ms() -> Group:
    group = Group(
        GroupType.ManuscriptGroup,
        "group_ms",
        {"ms_1"}
    )
    return group


@pytest.fixture
def group_txt() -> Group:
    group = Group(
        GroupType.TextGroup,
        "group_txt",
        {"txt_1"}
    )
    return group


@pytest.fixture
def group_ppl() -> Group:
    group = Group(
        GroupType.PersonGroup,
        "group_ppl",
        {"p_1", "p_2"}
    )
    return group


class TestGroups:

    def test_create_sql_engine_inmemory(self) -> None:
        e = database.get_engine(':memory:')
        assert isinstance(e, Engine)
        SQLModel.metadata.create_all(e)
        with Session(e) as s:
            assert s.info == {}

    def test_add_group_one(self, db: Database, group_ms: Group) -> None:
        db.add_group(group_ms)
        res = db.get_all_groups()
        assert len(res) == 1
        group_res = res.pop()
        assert group_res == group_ms

    def test_add_group_multiple(self, db: Database, group_ms: Group, group_txt: Group) -> None:
        db.add_group(group_ms)
        db.add_group(group_txt)
        res = db.get_all_groups()
        assert len(res) == 2
        assert group_ms in res
        assert group_txt in res

    def test_get_groups_empty(self, db: Database) -> None:
        res = db.get_all_groups()
        assert res == []
        res = db.get_ms_groups()
        assert res == []
        res = db.get_ppl_groups()
        assert res == []
        res = db.get_txt_groups()
        assert res == []

    def test_get_groups_non_empty(self, db: Database, group_ms: Group, group_txt: Group, group_ppl: Group) -> None:
        db.add_group(group_ms)
        db.add_group(group_txt)
        db.add_group(group_ppl)
        all = db.get_all_groups()
        assert group_ms in all
        assert group_txt in all
        assert group_ppl in all
        mss = db.get_ms_groups()
        assert mss == [group_ms]
        txt = db.get_txt_groups()
        assert txt == [group_txt]
        ppl = db.get_ppl_groups()
        assert ppl == [group_ppl]

    def test_update_group(self, db: Database, group_ms: Group) -> None:
        db.add_group(group_ms)
        assert db.get_all_groups() == [group_ms]
        new_items = group_ms.items.copy()
        new_items.add("ms_2")
        new_group = dataclasses.replace(group_ms, items=new_items)
        assert new_group != group_ms
        db.update_group(new_group, group_ms.group_id)
        res = db.get_all_groups()
        assert res == [new_group]
        assert group_ms not in res


# TODO-BL: add more tests here for rest of DB
