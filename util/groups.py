from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Union
import uuid
import os
import pickle
from enum import Enum
from util.constants import GROUPS_PATH_PICKLE


class GroupType(Enum):
    ManuscriptGroup = "msgroup"
    TextGroup = "txtgroup"
    PersonGroup = "persgroup"


@dataclass
class Group:
    group_type: GroupType
    name: str
    items: Set[str]
    date: datetime = datetime.now()
    group_id: uuid.UUID = uuid.uuid4()


@dataclass
class Groups:
    manuscript_groups: Dict[uuid.UUID, Group] = {}
    text_groups: Dict[uuid.UUID, Group] = {}
    person_groups: Dict[uuid.UUID, Group] = {}

    @staticmethod
    def from_cache() -> Optional[Groups]:
        if not os.path.exists(GROUPS_PATH_PICKLE):
            return None
        with open(GROUPS_PATH_PICKLE, 'rb') as f:
            g = pickle.load(f)
            if isinstance(g, Groups):
                return g
            return None

    def set(self, group: Group) -> None:
        if group.group_type == GroupType.ManuscriptGroup:
            self.manuscript_groups[group.group_id] = group
        if group.group_type == GroupType.TextGroup:
            self.text_groups[group.group_id] = group
        if group.group_type == GroupType.PersonGroup:
            self.person_groups[group.group_id] = group

    def remove(self, group: Union[Group, List[Group]]) -> None:
        gg = group if isinstance(group, list) else [group]
        for g in gg:
            if g.group_type == GroupType.ManuscriptGroup:
                self.manuscript_groups.pop(g.group_id)
            if g.group_type == GroupType.TextGroup:
                self.text_groups.pop(g.group_id)
            if g.group_type == GroupType.PersonGroup:
                self.person_groups.pop(g.group_id)

    def get_names(self, type: Optional[GroupType]) -> List[str]:
        if type == GroupType.ManuscriptGroup:
            return [g.name for _, g in self.manuscript_groups.items()]
        if type == GroupType.TextGroup:
            return [g.name for _, g in self.text_groups.items()]
        if type == GroupType.PersonGroup:
            return [g.name for _, g in self.person_groups.items()]
        return self.get_names(GroupType.ManuscriptGroup) + self.get_names(GroupType.PersonGroup) + self.get_names(GroupType.TextGroup)

    def get_group_by_name(self, name: str, type: Optional[GroupType]) -> Optional[Group]:
        if type == GroupType.ManuscriptGroup:
            for v in self.manuscript_groups.values():
                if v.name == name:
                    return v
            return None
        if type == GroupType.TextGroup:
            for v in self.text_groups.values():
                if v.name == name:
                    return v
            return None
        if type == GroupType.PersonGroup:
            for v in self.person_groups.values():
                if v.name == name:
                    return v
            return None
        return self.get_group_by_name(
            name, GroupType.ManuscriptGroup) or self.get_group_by_name(
            name, GroupType.TextGroup) or self.get_group_by_name(
            name, GroupType.PersonGroup)
