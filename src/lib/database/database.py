from typing import Protocol
from uuid import UUID

import pandas as pd

from src.lib.groups import Group
from src.lib.manuscripts import CatalogueEntry, Manuscript
from src.lib.people import Person


class Database(Protocol):
    def get_metadata(self, ms_ids: list[str]) -> pd.DataFrame:
        """One stop shop for simple search/SELECT queries.

        Args:
            table_name(str): Name of table to be queried
            column_name(str): Name of column on which to apply selection criteria
            search_criteria(List): What it is you are looking for

        Returns:
            pd.DataFrame
        """
        ...

    def ms_x_ppl(self, pers_ids: list[str]) -> list[str]:
        """Get IDs of all manuscripts related to a list of people."""
        ...

    def ppl_x_mss(self, ms_ids: list[str]) -> list[str]:
        """
        Get IDs of all people connected to a list of manuscripts.
        Returns list of IDs for people.
        """
        ...

    def ms_x_txts(self, txts: list[str]) -> list[str]:
        """
        Get IDs of all manuscripts connected to a list of texts.
        Returns list of IDs for manuscripts.
        """
        # TODO: clarify text definition
        ...

    def txts_x_ms(self, ms_ids: list[str]) -> list[str]:
        """
        Get IDs of all texts connected to a list of manuscripts.
        Returns list of IDs for texts.
        """
        # TODO: clarify text definition
        ...

    def persons_lookup_dict(self) -> dict[str, str]:
        """
        Gets the data from person(ID, first name, last name).
        Returns the lookup-dict for the IDs of people to their natural names.
        """
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
        """Adds all the data to the database"""
        ...
