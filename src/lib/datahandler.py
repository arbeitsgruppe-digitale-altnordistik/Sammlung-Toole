"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from lib import utils
from lib.database.database import Database
from lib.database.sqlite.database_sqlite_impl import DatabaseSQLiteImpl
from lib.groups import Group
from lib.utils import SearchOptions

log = utils.get_logger(__name__)


class DataHandler:

    manuscripts: dict[str, list[str]]
    """Lookup dictionary mapping full msIDs (handrit-IDs) to Shelfmarks, Nicknames of manuscripts."""

    texts: list[str]
    """Temporary lookup tool for search"""

    person_names: dict[str, str]
    """Name lookup dictionary mapping person IDs to the full name of the person"""

    person_names_inverse: dict[str, list[str]]
    """Inverse name lookup dictionary, mapping person names to a list of IDs of persons with said name"""

    database: Database
    """Database connector"""

    def __init__(self, database: Database) -> None:
        log.info("Creating new handler")
        self.database = database
        log.info("Databases up and running")
        self.person_names = self.database.persons_lookup_dict()
        self.person_names_inverse = _get_person_names_inverse(self.person_names)
        log.info("Loaded Person Info")
        self.manuscripts = self.database.ms_lookup_dict()
        log.info("Loaded MS Info")
        self.texts = self.database.txt_lookup_list()
        log.info("Loaded Text Info")
        log.info("Successfully created a Datahandler instance.")

    @staticmethod
    def make() -> DataHandler:
        """Create a DataHandler instance with a readily set-up database"""
        db = DatabaseSQLiteImpl()
        db.setup_db()
        return DataHandler(db)

    def search_manuscript_data(self, ms_ids: list[str]) -> pd.DataFrame:
        """Search manuscript metadata for manuscripts, given a list of manuscript IDs.

        Args:
            ms_ids (list[str]): a list of manuscript IDs

        Returns:
            pd.DataFrame: A dataframe containing the metadata for the requested manuscripts.
        """
        res = self.database.get_metadata(ms_ids)
        log.info(f"Found {len(res.index)} metadata entries for manuscripts: {ms_ids}")
        return res

    def search_manuscripts_containing_texts(self, texts: list[str], searchOption: SearchOptions) -> list[str]:
        """Search manuscripts containing certain texts

        Args:
            texts (list[str]): A list of text names
            searchOption (SearchOption): wether to do an AND or an OR search

        Returns:
            list[str]: A list of manuscripts IDs containing either one or all of the passed texts, depending on the chosen searchOption.
                Returns an empty list, if none were found.
        """
        log.info(f'Searching for manuscripts with texts: {texts} ({searchOption})')
        if not texts:
            log.debug('Searched texts are empty list')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            return _or_search(texts, self.database.ms_x_txts)
        else:
            return _and_search(texts, self.database.ms_x_txts)

    def search_texts_contained_by_manuscripts(self, ms_ids: list[str], searchOption: SearchOptions) -> list[str]:
        """Search the texts contained by certain manuscripts.

        Search for all texts contained by a given number of manuscripts.

        Depending on the search option, either the texts appearing in one of the named manuscripts,
        or the texts appearing in all manuscripts will be returned.

        Args:
            mss (list[str]): a list of manuscript full_id strings
            searchOption (SearchOptions):  wether to do an AND or an OR search

        Returns:
            list[str]: A list of text names.
        """
        log.info(f'Searching for texts contained by manuscripts: {ms_ids} ({searchOption})')
        if not ms_ids:
            log.debug('Searched for empty list of mss')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            return _or_search(ms_ids, self.database.txts_x_ms)
        else:
            return _and_search(ms_ids, self.database.txts_x_ms)

    def search_persons_related_to_manuscripts(self, ms_ids: list[str], searchOption: SearchOptions) -> list[str]:
        """Search for people related to a given list of manuscripts.

        Args:
            ms_ids (list[str]): Manuscript IDs
            searchOption (SearchOptions):  wether to do an AND or an OR search

        Returns:
            list[str]: a list of person IDs
        """
        log.info(f'Searching for persons related to manuscripts: {ms_ids} ({searchOption})')
        if not ms_ids:
            log.debug('Searched for empty list of mss')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            return _or_search(ms_ids, self.database.ppl_x_mss)
        else:
            return _and_search(ms_ids, self.database.ppl_x_mss)

    def search_manuscripts_related_to_persons(self, person_ids: list[str], search_option: SearchOptions) -> list[str]:
        """Search for manuscript related to a given list of people.

        Args:
            person_ids (list[str]):  list of person IDs
            search_option (SearchOptions): wether to do an AND or an OR search

        Returns:
            list[str]: a list of manuscript IDs
        """
        log.info(f'Searching for manuscript related to people: {person_ids} ({search_option})')
        if not person_ids:
            log.debug('Searched for empty list of people')
            return []
        if search_option == SearchOptions.CONTAINS_ONE:
            return _or_search(person_ids, self.database.ms_x_ppl)
        else:
            return _and_search(person_ids, self.database.ms_x_ppl)

    def get_all_groups(self) -> list[Group]:
        """Gets all groups from the DB"""
        return self.database.get_all_groups()

    def get_ms_groups(self) -> list[Group]:
        """Gets all manuscript groups from the DB"""
        return self.database.get_ms_groups()

    def get_ppl_groups(self) -> list[Group]:
        """Gets all people groups from the DB"""
        return self.database.get_ppl_groups()

    def get_txt_groups(self) -> list[Group]:
        """Gets all text groups from the DB"""
        return self.database.get_txt_groups()

    def put_group(self, group: Group) -> None:
        """Puts a group to the DB, replacing it if it already existed"""
        self.database.update_group(group, group.group_id)

    def add_group(self, group: Group) -> None:
        """Adds a new group to the DB."""
        self.database.add_group(group)


def _get_person_names_inverse(person_names: dict[str, str]) -> dict[str, list[str]]:
    res: dict[str, list[str]] = {}
    for k, v in person_names.items():
        res[v] = res.get(v, []) + [k]
    return res


def _and_search(params: list[str], search_fn: Callable[[list[str]], list[str]]) -> list[str]:
    """Helper method to do a logical AND search, provided a list of search parameters (IDs) and a search function to call."""
    sets = [set(search_fn([p])) for p in params]
    if not sets:
        log.info('nothing found')
        return []
    res = list(set.intersection(*sets))
    log.info(f'Search results: {len(res)}')
    return res


def _or_search(params: list[str], search_fn: Callable[[list[str]], list[str]]) -> list[str]:
    """Helper method to do a logical OR search, provided a list of search parameters (IDs) and a search function to call."""
    res = search_fn(params)
    log.info(f'Search results: {len(res)}')
    return res
