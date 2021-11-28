from dataclasses import dataclass
from datetime import datetime
from typing import List, Set
import uuid
from enum import Enum


class GroupType(Enum):
    ManuscriptGroup = 1
    TextGroup = 2
    PersonGroup = 3


@dataclass
class Group:
    group_type: GroupType
    name: str
    items: Set[str]
    date: datetime = datetime.now()
    group_id: uuid.UUID = uuid.uuid4()
