from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel, create_engine, select

from src.lib.constants import DATABASE_GROUPS_PATH
from src.lib.groups import Group, GroupType
from src.lib.models.group import GroupDBModel


class GroupsDB(Protocol):
    def setup_db(self) -> None:  # TODO: move this to a separate protocol
        """Creates the database and db tables if they don't exist yet."""
        ...

    def get_ms_groups(self) -> list[Group]:
        """Gets all groups of type `ManuscriptGroup` from the database."""
        ...

    def get_ppl_groups(self) -> list[Group]:
        """Gets all groups of type `PeopleGroup` from the database."""
        ...

    def get_txt_groups(self) -> list[Group]:
        """Gets all groups of type `TextGroup` from the database."""
        ...

    def get_all_groups(self) -> list[Group]:
        """Gets all groups from the database."""
        ...

    def add_group(self, group: Group) -> None:
        """Adds a new group to the database"""
        ...

    def update_group(self, group: Group, group_id: UUID) -> None:
        """Updates a group in the database, either replacing its previous version, or creating it anew."""
        ...


def get_engine(db_path: str = DATABASE_GROUPS_PATH) -> Engine:
    sqlite_url = f"sqlite:///{db_path}"
    return create_engine(sqlite_url)


@dataclass(frozen=True)
class GroupsDBImpl:
    engine: Engine = field(default_factory=get_engine, init=False)

    def setup_db(self) -> None:
        SQLModel.metadata.create_all(self.engine)

    def get_ms_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(GroupDBModel).where(GroupDBModel.group_type == GroupType.ManuscriptGroup)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def get_ppl_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(GroupDBModel).where(GroupDBModel.group_type == GroupType.PersonGroup)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def get_txt_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(GroupDBModel).where(GroupDBModel.group_type == GroupType.TextGroup)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def get_all_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(GroupDBModel)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def add_group(self, group: Group) -> None:
        with Session(self.engine) as session:
            db_model = GroupDBModel.make(group)
            session.add(db_model)
            session.commit()

    def update_group(self, group: Group, group_id: UUID) -> None:
        with Session(self.engine) as session:
            group_old = session.get(GroupDBModel, group_id)
            if group_old is not None:
                session.delete(group_old)
            group_new = GroupDBModel.make(group)
            session.add(group_new)
            session.commit()
