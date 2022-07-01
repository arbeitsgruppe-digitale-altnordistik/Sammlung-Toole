"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations

import os
import pickle
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

import util.tamer as tamer
from util import database, utils
from util.constants import *
from util.groups import Groups
from util.utils import GitUtil, SearchOptions, Settings

log = utils.get_logger(__name__)
settings = Settings.get_settings()


class DataHandler:

    manuscripts: dict[str, list[str]]
    """Lookup dictionary
    Dictionary mapping full msIDs (handrit-IDs) to Shelfmarks, Nicknames of manuscripts.
    """
    texts: list[str]
    """Temporary lookup tool for search"""
    # TODO: Come up with better solution -> Implement Tarrins unified names

    person_names: dict[str, str]
    """Name lookup dictionary

    Lookup dictionary mapping person IDs to the full name of the person
    """

    person_names_inverse: dict[str, list[str]]
    """Inverted name lookup dictionary
    
    Dictionary mapping person names to a list of IDs of persons with said name"""

    text_matrix: pd.DataFrame
    # """Text-Manuscript-Matrix

    # Sparse matrix with a row per manuscript and a column per text name.
    # True, if the manuscript contains the text.
    # Allows for lookups, which manuscripts a particular text is connected to.
    # """
    # TODO: Document the type of ID used for MSs in index/row label

    # person_matrix: pd.DataFrame
    # """Person-Manuscript-Matrix

    # Sparse matrix with a row per manuscript and a column per person ID.
    # True, if the manuscript is connected to the person (i.e. the description has the person tagged).
    # Allows for lookups, which manuscripts a particular person is connected to.
    # """
    # TODO: Still needed? Implemented as table in new backend. Delete?

    groups: Groups
    # CHORE: document

    def __init__(self) -> None:
        """DataHandler constructor.

        Returns a new instance of a DataHandler.

        Should not be called directly, but rather through the factory method `DataHandler.get_handler()`.
        """
        log.info("Creating new handler")
        self.person_names, self.person_names_inverse = DataHandler._load_persons()
        log.info("Loaded Person Info")
        self.manuscripts = DataHandler._load_ms_info()
        log.info("Loaded MS Info")
        # self.text_matrix = DataHandler._load_text_matrix(self.manuscripts)
        self.texts = DataHandler._load_txt_list()
        log.info("Loaded Text Info")
        # self.person_matrix = DataHandler._load_person_matrix(self.manuscripts)
        # log.info("Loaded Person-MSS-Matrix Info")
        self.groups = Groups.from_cache() or Groups()
        # self.manuscripts.drop(columns=["content", "soup"], inplace=True)
        log.info("Successfully created a Datahandler instance.")
        GitUtil.update_handler_state()

    # Static Methods
    # ==============

    @staticmethod
    def _from_pickle() -> Optional[DataHandler]:
        """Load datahandler from pickle, if available. Returns None otherwise."""
        pickle_path = Path(HANDLER_PATH_PICKLE)
        if pickle_path.exists():
            try:
                prev = sys.getrecursionlimit()
                with open(HANDLER_PATH_PICKLE, mode='rb') as file:
                    sys.setrecursionlimit(prev * 100)
                    obj = pickle.load(file)
                    sys.setrecursionlimit(prev)
                    if isinstance(obj, DataHandler):
                        obj.groups = Groups.from_cache() or Groups()
                        return obj
            except Exception:
                log.exception("Cound not load handler from pickle")
        return None

    @staticmethod
    def _load_ms_info() -> dict[str, list[str]]:
        """Load manuscript lookup dict from DB"""
        res = database.ms_lookup_dict(conn=database.create_connection())
        return res

    @staticmethod
    def _load_txt_list() -> list[str]:
        res = database.txt_lookup_list(conn=database.create_connection())
        return res

    @staticmethod
    def _load_persons() -> Tuple[dict[str, str], dict[str, list[str]]]:
        """Load person data"""
        person_names = database.persons_lookup_dict(conn=database.create_connection())
        return person_names, tamer.get_person_names_inverse(person_names)

    @staticmethod
    def is_cached() -> bool:
        """Check if the data handler should be available from cache."""
        return os.path.exists(HANDLER_PATH_PICKLE)

    @staticmethod
    def _build_db() -> None:
        dbConn = database.create_connection()
        database.db_set_up(dbConn)
        ppl = tamer.get_ppl_names()
        database.populate_people_table(dbConn, ppl)
        df = tamer.deliver_handler_data()
        df['soup'] = df['content'].apply(lambda x: BeautifulSoup(x, 'xml', from_encoding='utf-8'))
        msinfo = df['soup'].apply(lambda x: tamer.get_ms_info(x))
        df = df.join(msinfo)
        log.info("Loaded MS Info for new backend.")
        pplXmss = tamer.get_person_mss_matrix(df)
        txtXmss = tamer.get_text_mss_matrix(df)
        df = df.drop('soup', axis=1)
        df = df.drop('content', axis=1)
        database.populate_ms_table(dbConn, df)
        log.debug("Populated MS Table")
        database.populate_junctionPxM(dbConn, pplXmss)
        report = database.PxM_integrity_check(dbConn, pplXmss)
        if report == True:
            log.debug("Populated People x Manuscripts junction table.")
            print("Integrity check passed.")
        if report == False:
            log.error("Data integrity in ppl by MS matrix damaged. Duplicate entries or other types of corruption.")
            print("Integrity check failed.")
        database.populate_junctionTxM(conn=dbConn, incoming=txtXmss)
        dbConn.commit()
        dbConn.close()
        return

    # Class Methods
    # =============

    @classmethod
    def get_handler(cls) -> DataHandler:
        """Get a DataHandler

        Factory method to get a DataHandler object.

        Returns:
            DataHandler: A DataHandler, either loaded from cache or created anew.
        """
        log.info("Getting DataHandler")
        res: Optional[DataHandler] = cls._from_pickle()
        if not Path(DATABASE_PATH).exists():
            cls._build_db()
        if res:
            return res
        log.info("Could not get DataHandler from pickle")
        res = cls()
        res._to_pickle()
        log.info("DataHandler ready.")
        return res

    # Instance Methods
    # ================

    def _to_pickle(self) -> None:
        """Save the present DataHandler instance as pickle."""
        log.info("Saving handler to pickle")
        prev = sys.getrecursionlimit()
        with open(HANDLER_PATH_PICKLE, mode='wb') as file:
            try:
                sys.setrecursionlimit(prev * 100)
                pickle.dump(self, file)
                sys.setrecursionlimit(prev)
            except Exception:
                log.exception("Failed to pickle the data handler.")

    # API Methods
    # -----------

    def get_all_ppl_data(self) -> list[Tuple[str, str, str]]:
        res: list[Tuple[str, str, str]] = []
        with database.create_connection() as conn:
            cur = conn.cursor()
            for row in cur.execute('SELECT * FROM people ORDER BY persID'):
                res.append(row)
        return res

    def get_all_ppl_hrf(self) -> list[str]:
        res: list[str] = []
        with database.create_connection() as conn:
            cur = conn.cursor()
            for row in cur.execute('SELECT firstName, lastName FROM people ORDER BY firstName'):
                res.append(row)
        return res

    # def get_all_manuscript_data(self) -> pd.DataFrame:  # TODO: not used, right? (BL)
    #     """Get the manuscripts dataframe.

    #     Returns:
    #         A dataframe containing all manuscripts with their respective metadata.

    #         The dataframe will have the following structure:

    #         Per row, there will be metadata to one manuscript. The row indices are integers 0..n.

    #         The dataframe contains the following columns:

    #         - 'shelfmark'
    #         - 'shorttitle'
    #         - 'country'
    #         - 'settlement'
    #         - 'repository'
    #         - 'origin'
    #         - 'date'
    #         - 'Terminus post quem'
    #         - 'Terminus ante quem'
    #         - 'meandate'
    #         - 'yearrange'
    #         - 'support'
    #         - 'folio'
    #         - 'height'
    #         - 'width'
    #         - 'extent'
    #         - 'description'
    #         - 'creator'
    #         - 'id'
    #         - 'full_id'
    #         - 'filename'
    #     """
    #     with database.create_connection() as conn:
    #         res = pd.read_sql('SELECT * FROM manuscripts ORDER BY shelfmark', con=conn)
    #     return res

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
        return res

    def get_all_texts(self) -> pd.DataFrame:
        """return the text-manuscript-matrix"""
        return self.text_matrix

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
            res = database.ms_x_txts(conn=database.create_connection(), txts=texts)
            return res
        else:
            sets = []
            db = database.create_connection()
            for i in texts:
                ii = database.ms_x_txts(db, [i])
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
            res = database.txts_x_ms(conn=database.create_connection(), mss=Inmss)
            return res
        else:
            sets: list[set[str]] = []
            db = database.create_connection()
            for i in Inmss:
                sets = []
                db = database.create_connection()
                for i in Inmss:
                    ii = database.txts_x_ms(db, [i])
                    sets.append(set(ii))
                    print(ii)
            if not sets:
                log.info("No Texts found.")
                return []
            res = list(set.intersection(*sets))
            log.info(f'Search result: {res}')
            return res

    def get_person_name(self, pers_id: str) -> str:  # TODO: Might be obsolete with new backend?
        """Get a person's name, identified by the person's ID"""
        res = database.simple_people_search(conn=database.create_connection(), persID=pers_id)
        return res or ""

    def get_person_ids(self, pers_name: str) -> list[str]:
        """Get IDs of all persons with a certain name"""
        return self.person_names_inverse[pers_name]

    def search_persons_related_to_manuscripts(self, ms_full_ids: list[str], searchOption: SearchOptions) -> list[str]:
        # CHORE: Document 'else' clause: Relational division not implemented in SQL -> python hacky-whacky workaround # TODO: the hacky-whack should live in its own function
        log.info(f'Searching for persons related to manuscripts: {ms_full_ids} ({searchOption})')
        if not ms_full_ids:
            log.debug('Searched for empty list of mss')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            res = database.ppl_x_mss(conn=database.create_connection(), msIDs=ms_full_ids)
            return res
        else:
            sets = []
            db = database.create_connection()
            for i in ms_full_ids:
                ii = database.ppl_x_mss(db, [i])
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
            res = database.ms_x_ppl(conn=database.create_connection(DATABASE_PATH), pplIDs=person_ids)
            return res
        else:
            sets = []
            db = database.create_connection(DATABASE_PATH)
            for i in person_ids:
                ii = database.ms_x_ppl(db, [i])
                print(ii)
                sets.append(set(ii))
            print(sets)
            if not sets:
                log.info('no ms found')
                return []
            res = list(set.intersection(*sets))
            log.info(f'Search result: {res}')
            return res
