from __future__ import annotations

import os
import pickle
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Union

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


# @dataclass
# class Groups:
#     """Collects groups.

#     This class holds multiple groups. A dictionary per type of GroupType maps UUIDs to Group objects.

#     The class provides methods for accessing groups as well as for caching the entire Groups object.
#     """
#     manuscript_groups: Dict[uuid.UUID, Group] = field(default_factory=dict)
#     text_groups: Dict[uuid.UUID, Group] = field(default_factory=dict)
#     person_groups: Dict[uuid.UUID, Group] = field(default_factory=dict)

#     def set(self, group: Group) -> None:
#         """Set a Group value

#         Add the Group object to the respective dictionary, depending on the GroupType.

#         Will overwrite a existing group, if a Group with the same UUID already exists.

#         Args:
#             group (Group): The Group to be set (added/updated).
#         """
#         log.info(f"Set Group: {group.group_id} - {group.name} ({group.group_type})")
#         if group.group_type == GroupType.ManuscriptGroup:
#             self.manuscript_groups[group.group_id] = group
#             # log.debug(f"New manuscript group created: {group}")
#         elif group.group_type == GroupType.TextGroup:
#             self.text_groups[group.group_id] = group
#         elif group.group_type == GroupType.PersonGroup:
#             self.person_groups[group.group_id] = group
#         else:
#             log.warn(f"Something went wrong while saving group: {group.name}")
#             log.debug(f"Group Details: {group}")
#             log.debug(f"Currently Stored - MSS: {self.manuscript_groups}")
#             log.debug(f"Currently Stored - PPL: {self.person_groups}")
#             log.debug(f"Currently Stored - TXT: {self.text_groups}")
#         log.debug(f"Group contains new: ms={len(self.manuscript_groups)} txt={len(self.text_groups)} ppl={len(self.person_groups)}")

#     def remove(self, group: Union[Group, List[Group]]) -> None:
#         """Remove one or multiple Group objectss from the Groups instance.

#         Args:
#             group (Union[Group, List[Group]]): A Group or a List of Group objects to be removed.
#         """
#         gg = group if isinstance(group, list) else [group]
#         for g in gg:
#             if g.group_type == GroupType.ManuscriptGroup:
#                 self.manuscript_groups.pop(g.group_id)
#             if g.group_type == GroupType.TextGroup:
#                 self.text_groups.pop(g.group_id)
#             if g.group_type == GroupType.PersonGroup:
#                 self.person_groups.pop(g.group_id)

#     def get_names(self, type: Optional[GroupType]) -> List[str]:
#         """Gets the Group names for all the Groups stored in the instance, optionally only for one GroupType.

#         Args:
#             type (Optional[GroupType]): The GroupType to limit the search to. Optional. If none provided, all types will be considered.

#         Returns:
#             List[str]: A list of group names, possibly empty.
#         """
#         if type == GroupType.ManuscriptGroup:
#             return [g.name for _, g in self.manuscript_groups.items()]
#         if type == GroupType.TextGroup:
#             return [g.name for _, g in self.text_groups.items()]
#         if type == GroupType.PersonGroup:
#             return [g.name for _, g in self.person_groups.items()]
#         return self.get_names(GroupType.ManuscriptGroup) + self.get_names(GroupType.PersonGroup) + self.get_names(GroupType.TextGroup)

#     def get_group_by_name(self, name: str, type: Optional[GroupType]) -> Optional[Group]:
#         """Get a group by its name.

#         Args:
#             name (str): the group name to search for.
#             type (Optional[GroupType]): limit the search to a specific GroupType. Optional. If none provided, all types will be considered.

#         Returns:
#             Optional[Group]: A group fitting the name. None, if no Group with the specified name was found. If multiple groups share the same name, the first encounter will be returned.
#         """
#         if type == GroupType.ManuscriptGroup:
#             for v in self.manuscript_groups.values():
#                 if v.name == name:
#                     return v
#             return None
#         if type == GroupType.TextGroup:
#             for v in self.text_groups.values():
#                 if v.name == name:
#                     return v
#             return None
#         if type == GroupType.PersonGroup:
#             for v in self.person_groups.values():
#                 if v.name == name:
#                     return v
#             return None
#         return self.get_group_by_name(
#             name, GroupType.ManuscriptGroup) or self.get_group_by_name(
#             name, GroupType.TextGroup) or self.get_group_by_name(
#             name, GroupType.PersonGroup)
