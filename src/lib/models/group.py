from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel

from src.lib.groups import Group, GroupType


class GroupDBModel(SQLModel, table=True):
    group_id: UUID = Field(primary_key=True, default_factory=uuid4)
    group_type: GroupType
    name: str
    date: str
    items: str

    def to_group(self) -> Group:
        data = self.dict()
        data["date"] = datetime.fromtimestamp(float(self.date), timezone.utc).astimezone()
        data["items"] = set(self.items.split("|"))
        return Group(**data)

    @staticmethod
    def make(group: Group) -> GroupDBModel:
        data = {**group.__dict__}
        data["date"] = group.date.timestamp()
        data["items"] = '|'.join(group.items)
        return GroupDBModel(**data)
