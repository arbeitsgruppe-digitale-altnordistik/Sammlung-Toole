from typing import Protocol
from uuid import UUID

import pandas as pd

from src.lib.groups import Group
from src.lib.manuscripts import CatalogueEntry, Manuscript
from src.lib.people import Person


class Database(Protocol):
    def get_metadata(self, ms_ids: list[str]) -> pd.DataFrame:
        """Get a dataframe of manuscript metadata, given a list of manuscript IDs."""
        ...

    def ms_x_ppl(self, pers_ids: list[str]) -> list[str]:
        """Get a list of manuscript IDs related to a given list of people."""
        ...

    def ppl_x_mss(self, ms_ids: list[str]) -> list[str]:
        """Get a list of people IDs related to a given list of manuscripts."""
        ...

    def ms_x_txts(self, txts: list[str]) -> list[str]:
        """Get a list of manuscript IDs containing a given list of texts."""
        ...

    def txts_x_ms(self, ms_ids: list[str]) -> list[str]:
        """Get a list of texts contained by a given list of manuscripts."""
        ...

    def persons_lookup_dict(self) -> dict[str, str]:
        """Returns the lookup-dict for the IDs of people to their full names."""
        ...

    def ms_lookup_dict(self) -> dict[str, list[str]]:
        """Returns the lookup-dict for the IDs of manuscripts to their human readable signatures."""
        ...

    def txt_lookup_list(self) -> list[str]:
        """Returns the lookup-list for the texts. Used in front end search."""
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

    def add_data(self, people: list[Person], catalogue_entries: list[CatalogueEntry], manuscripts: list[Manuscript]) -> None:
        """Adds all the data to the database, used for initialization."""
        ...
