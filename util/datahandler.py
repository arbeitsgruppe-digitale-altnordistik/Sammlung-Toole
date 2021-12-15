"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations

import os
from pathlib import Path
import pickle
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from scipy import sparse

import util.tamer as tamer
from util import database, utils
from util.constants import *
from util.groups import Groups
from util.utils import GitUtil, SearchOptions, Settings

log = utils.get_logger(__name__)
settings = Settings.get_settings()


class DataHandler:

    manuscripts: pd.DataFrame
    """Manuscripts
    
    A dataframe containing all manuscripts with their respective metadata.
    
    The dataframe will have the following structure:
    
    Per row, there will be metadata to one manuscript. The row indices are integers 0..n.
    
    The dataframe contains the following columns:
    
    - 'shelfmark'
    - 'shorttitle'
    - 'country'
    - 'settlement'
    - 'repository'
    - 'origin'
    - 'date'
    - 'Terminus post quem'
    - 'Terminus ante quem'
    - 'meandate'
    - 'yearrange'
    - 'support'
    - 'folio'
    - 'height'
    - 'width'
    - 'extent'
    - 'description'
    - 'creator'
    - 'id'
    - 'full_id'
    - 'filename'
    """

    person_names: Dict[str, str]
    """Name lookup dictionary

    Lookup dictionary mapping person IDs to the full name of the person
    """

    person_names_inverse: Dict[str, List[str]]
    """Inverted name lookup dictionary
    
    Dictionary mapping person names to a list of IDs of persons with said name"""

    text_matrix: pd.DataFrame
    """Text-Manuscript-Matrix

    Sparse matrix with a row per manuscript and a column per text name.
    True, if the manuscript contains the text.
    Allows for lookups, which manuscripts a particular text is connected to.
    """  # TODO: Document the type of ID used for MSs in index/row label

    person_matrix: pd.DataFrame
    """Person-Manuscript-Matrix

    Sparse matrix with a row per manuscript and a column per person ID.
    True, if the manuscript is connected to the person (i.e. the description has the person tagged).
    Allows for lookups, which manuscripts a particular person is connected to.
    """

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
        self.manuscripts = DataHandler._load_ms_info(persons=self.person_names)
        log.info("Loaded MS Info")
        self.text_matrix = DataHandler._load_text_matrix(self.manuscripts)
        log.info("Loaded Text Info")
        self.person_matrix = DataHandler._load_person_matrix(self.manuscripts)
        log.info("Loaded Person-MSS-Matrix Info")
        self.groups = Groups.from_cache() or Groups()
        log.debug(f"Groups loaded: {self.groups}")
        self.manuscripts.drop(columns=["content", "soup"], inplace=True)
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
                        log.debug(f"Groups loaded: {obj.groups}")
                        return obj
            except Exception:
                log.exception("Cound not load handler from pickle")
        return None

    @staticmethod
    def _load_ms_info(persons: Dict[str, str]) -> pd.DataFrame:
        """Load manuscript metadata"""
        df = tamer.deliver_handler_data()
        df['soup'] = df['content'].apply(lambda x: BeautifulSoup(x, 'xml', from_encoding='utf-8'))
        msinfo = df['soup'].apply(lambda x: tamer.get_msinfo(x, persons))
        log.info("Loaded MS Info for new backend.")
        df = df.join(msinfo)
        return df

    @staticmethod
    def _load_text_matrix(df: pd.DataFrame) -> pd.DataFrame:
        """Load the text-manuscript-matrix"""
        mss_ids, text_names, coords = tamer.get_text_mss_matrix_coordinatres(df)
        r, c = map(list, zip(*coords))
        row = np.array(r)
        col = np.array(c)
        data = np.array([True]*len(row))
        matrix = sparse.coo_matrix((data, (row, col)))
        df = pd.DataFrame.sparse.from_spmatrix(matrix, index=mss_ids, columns=text_names)
        return df

    @staticmethod
    def _load_person_matrix(df: pd.DataFrame) -> pd.DataFrame:
        """Load the person-manuscript-matrix"""
        mss_ids, pers_ids, coords = tamer.get_person_mss_matrix_coordinatres(df)
        r, c = map(list, zip(*coords))
        row = np.array(r)
        col = np.array(c)
        data = np.array([True]*len(row))
        matrix = sparse.coo_matrix((data, (row, col)))
        df = pd.DataFrame.sparse.from_spmatrix(matrix, index=mss_ids, columns=pers_ids)
        return df

    @staticmethod
    def _load_persons() -> Tuple[Dict[str, str], Dict[str, List[str]]]:
        """Load person data"""
        person_names = tamer.get_person_names()
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
        log.info("Loaded MS Info for new backend.")
        df = df.drop('soup', axis=1)
        df = df.drop('content', axis=1)
        df = df.join(msinfo)
        import pdb
        pdb.set_trace()
        database.populate_ms_table(dbConn, df)
        cur = dbConn.cursor()
        for row in cur.execute('SELECT * FROM manuscripts ORDER BY shelfmark'):
            print(row)
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

    def get_all_ppl_data(self) -> List[Tuple[str, str, str]]:
        res: List[Tuple[str, str, str]] = []
        with database.create_connection() as conn:
            cur = conn.cursor()
            for row in cur.execute('SELECT * FROM people ORDER BY persID'):
                res.append(row)
        return res

    def get_all_manuscript_data(self) -> pd.DataFrame:
        """Get the manuscripts dataframe.

        Returns:
            A dataframe containing all manuscripts with their respective metadata.

            The dataframe will have the following structure:

            Per row, there will be metadata to one manuscript. The row indices are integers 0..n.

            The dataframe contains the following columns:

            - 'shelfmark'
            - 'shorttitle'
            - 'country'
            - 'settlement'
            - 'repository'
            - 'origin'
            - 'date'
            - 'Terminus post quem'
            - 'Terminus ante quem'
            - 'meandate'
            - 'yearrange'
            - 'support'
            - 'folio'
            - 'height'
            - 'width'
            - 'extent'
            - 'description'
            - 'creator'
            - 'id'
            - 'full_id'
            - 'filename'
        """
        with database.create_connection() as conn:
            res = pd.read_sql('SELECT * FROM manuscripts ORDER BY shelfmark', con=conn)
        return res

    def search_manuscript_data(self,
                               full_ids: Union[List[str], pd.Series, pd.DataFrame] = None,
                               ms_ids: Union[List[str], pd.Series, pd.DataFrame] = None,
                               shelfmarks: Union[List[str], pd.Series, pd.DataFrame] = None,
                               filenames: Union[List[str], pd.Series, pd.DataFrame] = None) -> Optional[pd.DataFrame]:
        """Search manuscript metadata for certain manuscripts.

        Basic search function:

        Searches for manuscripts with a certain IDs, and returns the metadata for the respective manuscripts.

        IDs can either be full_id (i.e. a certain catalogue entry),
        ms_ids (i.e. a certain manuscript that can have catalogue entries in multiple languages)
        shelfmarks (which will possibly yield multiple results per shelfmark)
        or filenames (refers to the XML files of the catalogue entry).

        Note: Exactly one of the four optional parameters should be passed.

        Args:
            full_ids (Union[List[str], pd.Series, pd.DataFrame], optional): List/Series/Dataframe of catalogue entry IDs. Defaults to None.
            ms_ids (Union[List[str], pd.Series, pd.DataFrame], optional): List/Series/Dataframe of manuscript IDs. Defaults to None.
            shelfmarks (Union[List[str], pd.Series, pd.DataFrame], optional): List/Series/Dataframe of manuscript IDs. Defaults to None.
            filenames (Union[List[str], pd.Series, pd.DataFrame], optional): List/Series/Dataframe of XML file names. Defaults to None.

        Returns:
            Optional[pd.DataFrame]: A dataframe containing the metadata for the requested manuscripts. 
                Returns None if no manuscript was found or if no parameters were passed.
        """
        log.info(f'Searching for manuscripts: {full_ids}/{ms_ids}/{filenames}')
        # full id
        if full_ids is not None:
            if isinstance(full_ids, list) and full_ids:
                return self.manuscripts.loc[self.manuscripts['full_id'].isin(full_ids)]
            elif isinstance(full_ids, pd.DataFrame):
                if full_ids.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['full_id'].isin(full_ids['full_id'])]
            elif isinstance(full_ids, pd.Series):
                if full_ids.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['full_id'].isin(full_ids)]
        # id
        elif ms_ids is not None:
            if isinstance(ms_ids, list) and ms_ids:
                return self.manuscripts.loc[self.manuscripts['id'].isin(ms_ids)]
            elif isinstance(ms_ids, pd.DataFrame):
                if ms_ids.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['id'].isin(ms_ids['id'])]
            elif isinstance(ms_ids, pd.Series):
                if ms_ids.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['id'].isin(ms_ids)]
        # filename
        elif filenames is not None:
            if isinstance(filenames, list) and filenames:
                return self.manuscripts.loc[self.manuscripts['filename'].isin(filenames)]
            elif isinstance(filenames, pd.DataFrame):
                if filenames.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['filename'].isin(filenames['filename'])]
            elif isinstance(filenames, pd.Series):
                if filenames.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['filename'].isin(filenames)]
        # shelfmark
        elif shelfmarks is not None:
            if isinstance(shelfmarks, list) and shelfmarks:
                return self.manuscripts.loc[self.manuscripts['shelfmark'].isin(shelfmarks)]
            elif isinstance(shelfmarks, pd.DataFrame):
                if shelfmarks.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['shelfmark'].isin(shelfmarks['shelfmark'])]
            elif isinstance(shelfmarks, pd.Series):
                if shelfmarks.empty:
                    return None
                return self.manuscripts.loc[self.manuscripts['shelfmark'].isin(shelfmarks)]
        # no argument passed
        return None

    def get_all_texts(self) -> pd.DataFrame:
        """return the text-manuscript-matrix"""
        return self.text_matrix

    def search_manuscripts_containing_texts(self, texts: List[str], searchOption: SearchOptions) -> List[str]:
        """Search manuscripts containing certain texts

        Args:
            texts (List[str]): A list of text names
            searchOption (SearchOption): wether to do an AND or an OR search

        Returns:
            List[str]: A list of `full_id`s of manuscripts containing either one or all of the passed texts, depending on the chosen searchOption.
                Returns an empty list, if none were found.
        """
        log.info(f'Searching for manuscripts with texts: {texts} ({searchOption})')
        if not texts:
            log.debug('Searched texts are empty list')
            return []
        if searchOption == SearchOptions.CONTAINS_ONE:
            hits = []
            for t in texts:
                df = self.text_matrix[self.text_matrix[t] == True]
                mss = list(df.index)
                hits += mss
            res_ = list(set(hits))
            _res = self.manuscripts[self.manuscripts['full_id'].isin(res_)]
            res = list(set(_res['shelfmark'].tolist()))
            if not res:
                log.info('no manuscripts found')
                return []
            log.info(f'Search result: {res}')
            return res
        else:
            hits = []
            for t in texts:
                df = self.text_matrix[self.text_matrix[t] == True]
                s = set(df.index)
                hits.append(s)
            if not hits:
                log.info('no manuscripts fond')
                return []
            intersection = set.intersection(*hits)
            res_ = list(intersection)
            _res = self.manuscripts[self.manuscripts['full_id'].isin(res_)]
            res = list(set(_res['shelfmark'].tolist()))
            log.info(f'Search result: {res}')
            return res

    def search_texts_contained_by_manuscripts(self, Inmss: List[str], searchOption: SearchOptions) -> List[str]:
        """Search the texts contained by certain manuscripts.

        Search for all texts contained by a given number of manuscripts.

        Depending on the search option, either the texts appearing in one of the named manuscripts,
        or the texts appearing in all manuscripts will be returned.

        Args:
            mss (List[str]): a list of manuscript full_id strings
            searchOption (SearchOptions):  wether to do an AND or an OR search

        Returns:
            List[str]: A list of text names.
        """
        log.info(f'Searching for texts contained by manuscripts: {Inmss} ({searchOption})')
        if not Inmss:
            log.debug('Searched for empty list of mss')
            return []
        mss_ = self.manuscripts[self.manuscripts['full_id'].isin(Inmss)]
        mss = mss_['full_id'].tolist()
        df = self.text_matrix.transpose()
        if searchOption == SearchOptions.CONTAINS_ONE:
            hits = []
            for ms in mss:
                d = df[df[ms] == True]
                mss = list(d.index)
                hits += mss
            res = list(set(hits))
            if not res:
                log.info('no texts found')
                return []
            log.info(f'Search result: {res}')
            return res
        else:
            sets = []
            for ms in mss:
                d = df[df[ms] == True]
                s = set(d.index)
                sets.append(s)
            if not sets:
                log.info('no texts found')
                return []
            intersection = set.intersection(*sets)
            res = list(intersection)
            log.info(f'Search result: {res}')
            return res

    def get_ms_urls_from_search_or_browse_urls(self, urls: List[str], sharedMode: bool = False) -> Tuple[List[str], pd.DataFrame]:
        # CHORE: documentation
        # TODO: should probably be moved to tamer, right?
        msss: List[pd.DataFrame] = []
        for url in urls:
            if "/search/results/" in url:
                pages = tamer.get_search_result_pages(url)
                shelfmarks = tamer.get_shelfmarks_from_urls(pages)
                log.info(f"Loaded Shelfmarks: {shelfmarks}")
                mss = self.manuscripts[self.manuscripts['shelfmark'].isin(shelfmarks)]
            else:
                ids = tamer.efnisordResult(url)
                mss = self.manuscripts[self.manuscripts['id'].isin(ids)]
            msss.append(mss)

        if sharedMode:  # Looked it over, they don't return the same. I got confused between this branch and stable (stable squishes results in tamer).
            res = self.manuscripts
            for df in msss:
                res = pd.merge(res, df, on='shelfmark', how='inner')
            return list(res['shelfmark']), res
        else:
            all_hits: pd.DataFrame = pd.concat(msss)
            unique_hits = all_hits.drop_duplicates().reset_index(drop=True)
            return list(unique_hits['shelfmark']), unique_hits

    def get_person_name(self, pers_id: str) -> str:
        """Get a person's name, identified by the person's ID"""
        return self.person_names.get(pers_id) or ""

    def get_person_ids(self, pers_name: str) -> List[str]:
        """Get IDs of all persons with a certain name"""
        return self.person_names_inverse[pers_name]

    def search_persons_related_to_manuscripts(self, ms_full_ids: List[str], searchOption: SearchOptions) -> List[str]:
        log.info(f'Searching for persons related to manuscripts: {ms_full_ids} ({searchOption})')
        if not ms_full_ids:
            log.debug('Searched for empty list of mss')
            return []
        df = self.person_matrix.transpose()
        if searchOption == SearchOptions.CONTAINS_ONE:
            hits = []
            for ms in ms_full_ids:
                d = df[df[ms] == True]
                mss = list(d.index)
                hits += mss
            res = list(set(hits))
            if not res:
                log.info('no person found')
                return []
            log.info(f'Search result: {res}')
            return res
        else:
            sets = []
            for ms in ms_full_ids:
                d = df[df[ms] == True]
                s = set(d.index)
                sets.append(s)
            if not sets:
                log.info('no person fond')
                return []
            intersection = set.intersection(*sets)
            res = list(intersection)
            log.info(f'Search result: {res}')
            return res

    def search_manuscripts_related_to_persons(self, person_ids: List[str], searchOption: SearchOptions) -> List[str]:
        # CHORE: Document
        log.info(f'Searching for manuscript related to people: {person_ids} ({searchOption})')
        if not person_ids:
            log.debug('Searched for empty list of ppl')
            return []
        df = self.person_matrix
        if searchOption == SearchOptions.CONTAINS_ONE:
            hits = []
            for pers in person_ids:
                d = df[df[pers] == True]
                mss = list(d.index)
                hits += mss
            res = list(set(hits))
            if not res:
                log.info('no ms found')
                return []
            log.info(f'Search result: {res}')
            return res
        else:
            sets = []
            for pers in person_ids:
                d = df[df[pers] == True]
                s = set(d.index)
                sets.append(s)
            if not sets:
                log.info('no ms fond')
                return []
            intersection = set.intersection(*sets)
            res = list(intersection)
            log.info(f'Search result: {res}')
            return res
