"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd
from src.lib import utils
from src.lib.constants import (DATABASE_GROUPS_PATH, DATABASE_PATH,
                               XML_BASE_PATH)
from src.lib.database import (database, db_init, deduplicate, groups_database,
                              groups_db_init)
from src.lib.groups import Group
from src.lib.utils import GitUtil, SearchOptions, Settings
from src.lib.xml import tamer

log = utils.get_logger(__name__)
settings = Settings.get_settings()

# TODO: get rid of some of the code duplications in this file?


class DataHandler:

    manuscripts: dict[str, list[str]]
    """Lookup dictionary mapping full msIDs (handrit-IDs) to Shelfmarks, Nicknames of manuscripts."""

    texts: list[str]
    """Temporary lookup tool for search"""
    # TODO: Come up with better solution -> Implement Tarrins unified names

    person_names: dict[str, str]
    """Name lookup dictionary mapping person IDs to the full name of the person"""

    person_names_inverse: dict[str, list[str]]
    """Inverse name lookup dictionary, mapping person names to a list of IDs of persons with said name"""

    def __init__(self) -> None:
        if not Path(DATABASE_PATH).exists():
            DataHandler._build_db()
        if not Path(DATABASE_GROUPS_PATH).exists():
            DataHandler._build_groups_db()

        log.info("Creating new handler")
        self.person_names, self.person_names_inverse = DataHandler._load_persons()
        log.info("Loaded Person Info")
        self.manuscripts = database.ms_lookup_dict(database.create_connection().cursor())
        log.info("Loaded MS Info")
        self.texts = database.txt_lookup_list(database.create_connection().cursor())
        log.info("Loaded Text Info")
        log.info("Successfully created a Datahandler instance.")
        GitUtil.update_handler_state()

    # Static Methods
    # ==============

    @staticmethod
    def _load_persons() -> Tuple[dict[str, str], dict[str, list[str]]]:
        """Load person data"""
        person_names = database.persons_lookup_dict(database.create_connection().cursor())
        return person_names, _get_person_names_inverse(person_names)

    @staticmethod
    def _build_groups_db() -> None:
        with groups_database.create_connection() as con:
            groups_db_init.db_set_up(con)
            log.info("Built groups database")

    @staticmethod
    def _build_db() -> None:
        with database.create_connection(":memory:") as db_conn:
            db_init.db_set_up(db_conn)
            ppl = tamer.get_ppl_names()
            db_init.populate_people_table(db_conn, ppl)
            files = Path(XML_BASE_PATH).rglob('*.xml')
            # files = list(Path(XML_BASE_PATH).rglob('*.xml'))[:100]
            ms_meta, msppl, mstxts = tamer.get_metadata_from_files(files)
            db_init.populate_ms_table(db_conn, ms_meta)
            ms_ppl = [x for y in msppl for x in y if x[2] != 'N/A']
            ms_txts = [x for y in mstxts for x in y if x[2] != "N/A"]  # TODO-BL: I'd like to get rid of "N/A"
            db_init.populate_junction_pxm(db_conn, ms_ppl)
            db_init.populate_junction_txm(db_conn, ms_txts)
            unified_metadata = deduplicate.get_unified_metadata(ms_meta)
            db_init.populate_unified_ms_table(db_conn, unified_metadata)
            db_init.populate_junction_pxm_unified(db_conn, ms_ppl)
            db_init.populate_junction_txm_unified(db_conn, ms_txts)
            with database.create_connection(DATABASE_PATH) as dest_conn:
                db_conn.backup(dest_conn)

    # Instance Methods
    # ================

    # API Methods
    # -----------

    def get_all_ppl_data(self) -> list[Tuple[str, str, str]]:
        res: list[Tuple[str, str, str]] = []
        with database.create_connection() as conn:
            cur = conn.cursor()
            for row in cur.execute('SELECT * FROM people ORDER BY persID'):
                res.append(row)
        return res

    def search_manuscript_data(self, mssIDs: list[str]) -> pd.DataFrame:
        """Search manuscript metadata for certain manuscripts.

        Basic search function:

        Searches for manuscripts with a certain IDs, and returns the metadata for the respective manuscripts.

        IDs can either be full_id (i.e. a certain catalogue entry),
        ms_ids (i.e. a certain manuscript that can have catalogue entries in multiple languages)
        shelfmarks (which will possibly yield multiple results per shelfmark)
        or filenames (refers to the XML files of the catalogue entry).

        Note: Exactly one of the four optional parameters should be passed.

        Args:
            full_ids (Union[list[str], pd.Series, pd.DataFrame], optional): list/Series/Dataframe of catalogue entry IDs. Defaults to None.
            ms_ids (Union[list[str], pd.Series, pd.DataFrame], optional): list/Series/Dataframe of manuscript IDs. Defaults to None.
            shelfmarks (Union[list[str], pd.Series, pd.DataFrame], optional): list/Series/Dataframe of manuscript IDs. Defaults to None.
            filenames (Union[list[str], pd.Series, pd.DataFrame], optional): list/Series/Dataframe of XML file names. Defaults to None.

        Returns:
            Optional[pd.DataFrame]: A dataframe containing the metadata for the requested manuscripts.
                Returns None if no manuscript was found or if no parameters were passed.
        """
        db = database.create_connection()
        res = database.get_metadata(table_name="manuscripts", column_name="full_id", search_criteria=mssIDs, conn=db)
        # FIXME-BL: docstring got outdated and is now lying
        return res

    def search_manuscripts_containing_texts(self, texts: list[str], searchOption: SearchOptions) -> list[str]:
        """Search manuscripts containing certain texts

        Args:
            texts (list[str]): A list of text names
            searchOption (SearchOption): wether to do an AND or an OR search

        Returns:
            list[str]: A list of `full_id`s of manuscripts containing either one or all of the passed texts, depending on the chosen searchOption.
                Returns an empty list, if none were found.
        """
        log.info(f'Searching for manuscripts with texts: {texts} ({searchOption})')
        if not texts:
            log.debug('Searched texts are empty list')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            res = database.ms_x_txts(cursor=database.create_connection().cursor(), txts=texts)
            return res
        else:
            sets = []
            db = database.create_connection()
            for i in texts:
                ii = database.ms_x_txts(db.cursor(), [i])
                sets.append(set(ii))
            if not sets:
                log.info('no ms found')
                return []
            res = list(set.intersection(*sets))
            log.info(f'Search result: {res}')
            return res

    def search_texts_contained_by_manuscripts(self, Inmss: list[str], searchOption: SearchOptions) -> list[str]:
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
        log.info(f'Searching for texts contained by manuscripts: {Inmss} ({searchOption})')
        if not Inmss:
            log.debug('Searched for empty list of mss')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            res = database.txts_x_ms(cursor=database.create_connection().cursor(), ms_ids=Inmss)
            return res
        else:
            sets: list[set[str]] = []
            db = database.create_connection()
            for i in Inmss:
                sets = []
                for i in Inmss:
                    ii = database.txts_x_ms(db.cursor(), [i])
                    sets.append(set(ii))
            if not sets:
                log.info("No Texts found.")
                return []
            res = list(set.intersection(*sets))
            log.info(f'Search result: {res}')
            return res

    def search_persons_related_to_manuscripts(self, ms_full_ids: list[str], searchOption: SearchOptions) -> list[str]:
        # CHORE: Document 'else' clause: Relational division not implemented in SQL -> python hacky-whacky workaround # TODO: the hacky-whack should live in its own function
        log.info(f'Searching for persons related to manuscripts: {ms_full_ids} ({searchOption})')
        if not ms_full_ids:
            log.debug('Searched for empty list of mss')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            res = database.ppl_x_mss(cursor=database.create_connection().cursor(), ms_ids=ms_full_ids)
            return res
        else:
            sets = []
            db = database.create_connection()
            for i in ms_full_ids:
                ii = database.ppl_x_mss(db.cursor(), [i])
                sets.append(set(ii))
            if not sets:
                log.info('no ms found')
                return []
            res = list(set.intersection(*sets))
            log.info(f'Search result: {res}')
            return res

    def search_manuscripts_related_to_persons(self, person_ids: list[str], searchOption: SearchOptions) -> list[str]:
        # CHORE: Document
        # CHORE: Document 'else' clause: Relational division not implemented in SQL -> python hacky-whacky workaround
        log.info(f'Searching for manuscript related to people: {person_ids} ({searchOption})')

        if not person_ids:
            log.debug('Searched for empty list of ppl')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            res = database.ms_x_ppl(cursor=database.create_connection().cursor(), pers_ids=person_ids)
            return res
        else:
            sets = []
            db = database.create_connection()
            for i in person_ids:
                ii = database.ms_x_ppl(db.cursor(), [i])
                sets.append(set(ii))
            if not sets:
                log.info('no ms found')
                return []
            res = list(set.intersection(*sets))
            log.info(f'Search result: {res}')
            return res

    def get_all_groups(self) -> list[Group]:
        with groups_database.create_connection() as con:
            cur = con.cursor()
            return groups_database.get_all_groups(cur)

    def get_ms_groups(self) -> list[Group]:
        with groups_database.create_connection() as con:
            cur = con.cursor()
            return groups_database.get_ms_groups(cur)

    def get_ppl_groups(self) -> list[Group]:
        with groups_database.create_connection() as con:
            cur = con.cursor()
            return groups_database.get_ppl_groups(cur)

    def get_txt_groups(self) -> list[Group]:
        with groups_database.create_connection() as con:
            cur = con.cursor()
            return groups_database.get_txt_groups(cur)

    def put_group(self, group: Group) -> None:
        with groups_database.create_connection() as con:
            groups_database.put_group(con, group)

    # def get_group_names(self, gtype: Optional[GroupType] = None) -> list[str]:
    #     with groups_database.create_connection() as con:
    #         return groups_database.get_group_names(con, gtype)

    # def get_group_by_name(self, name: str, gtype: Optional[GroupType] = None) -> Optional[Group]:
    #     with groups_database.create_connection() as con:
    #         return groups_database.get_group_by_name(con, name, gtype)


def _get_person_names_inverse(person_names: dict[str, str]) -> dict[str, list[str]]:
    res: dict[str, list[str]] = {}
    for k, v in person_names.items():
        res[v] = res.get(v, []) + [k]
    return res
