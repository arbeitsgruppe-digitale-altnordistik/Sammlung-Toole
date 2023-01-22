from uuid import UUID

import streamlit as st
from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel, create_engine, select

from src.lib.constants import DATABASE_GROUPS_PATH
from src.lib.groups import Group, GroupType
from src.lib.models.group import GroupDBModel


@st.experimental_singleton
def get_engine(db_path: str = DATABASE_GROUPS_PATH) -> Engine:
    sqlite_url = f"sqlite:///{db_path}"
    return create_engine(sqlite_url)


def setup_db(engine: Engine = get_engine()) -> None:
    SQLModel.metadata.create_all(engine)


def get_ms_groups(engine: Engine = get_engine()) -> list[Group]:
    with Session(engine) as session:
        statement = select(GroupDBModel).where(GroupDBModel.group_type == GroupType.ManuscriptGroup)
        groups = session.exec(statement)
        return [g.to_group() for g in groups]


def get_ppl_groups(engine: Engine = get_engine()) -> list[Group]:
    with Session(engine) as session:
        statement = select(GroupDBModel).where(GroupDBModel.group_type == GroupType.PersonGroup)
        groups = session.exec(statement)
        return [g.to_group() for g in groups]


def get_txt_groups(engine: Engine = get_engine()) -> list[Group]:
    with Session(engine) as session:
        statement = select(GroupDBModel).where(GroupDBModel.group_type == GroupType.TextGroup)
        groups = session.exec(statement)
        return [g.to_group() for g in groups]


def get_all_groups(engine: Engine = get_engine()) -> list[Group]:
    with Session(engine) as session:
        statement = select(GroupDBModel)
        groups = session.exec(statement)
        return [g.to_group() for g in groups]


def add_group(group: Group, engine: Engine = get_engine()) -> None:
    with Session(engine) as session:
        db_model = GroupDBModel.make(group)
        session.add(db_model)
        session.commit()


def update_group(group: Group, group_id: UUID, engine: Engine = get_engine()) -> None:
    with Session(engine) as session:
        group_old = session.get(GroupDBModel, group_id)
        if group_old is not None:
            session.delete(group_old)
        group_new = GroupDBModel.make(group)
        session.add(group_new)
        session.commit()
