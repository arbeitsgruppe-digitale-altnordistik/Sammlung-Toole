from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Set

from src.lib import utils

log = utils.get_logger(__name__)


class GroupType(Enum):
    """Enum defining the type of a group"""
    ManuscriptGroup = "msgroup"
    TextGroup = "txtgroup"
    PersonGroup = "persgroup"

    @staticmethod
    def from_string(s: str) -> GroupType:
        if s == GroupType.ManuscriptGroup.value:
            return GroupType.ManuscriptGroup
        elif s == GroupType.TextGroup.value:
            return GroupType.TextGroup
        elif s == GroupType.PersonGroup.value:
            return GroupType.PersonGroup
        else:
            raise ValueError(f"Invalid group type: {s}")


@dataclass
class Group:
    """Group dataclass

    Represents a group of search results of a certain type.

    Args:
        group_type (GroupType): the type of object grouped in this group.
        name (str): the human readable name of the group.
        date (datetime, optional): creation date of the group. Optional. If not provided, the current instant will be used.
        group_id (UUID, optional): unique ID of the groupe. Optional. If not provbided, a new random UUID will be generated.
    """
    group_type: GroupType
    name: str
    items: Set[str]
    date: datetime = field(default_factory=lambda: datetime.now(timezone.utc).astimezone())
    group_id: uuid.UUID = field(default_factory=uuid.uuid4)
