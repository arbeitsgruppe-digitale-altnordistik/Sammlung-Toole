"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations

import json
import os
import pickle
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from scipy import sparse

import util.tamer as tamer
from util import metadata, utils
from util.constants import *
from util.groups import Group, Groups, GroupType
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

    def __init__(self,
                 manuscripts: pd.DataFrame = None,
                 texts: pd.DataFrame = None,
                 xmls: Optional[pd.DataFrame] = None,
                 contents: Optional[pd.DataFrame] = None):
        # CHORE: document
        log.info("Creating new handler")
        self.person_names, self.person_names_inverse = DataHandler._load_persons()
        log.info("Loaded Person Info")
        self.manuscripts = manuscripts if manuscripts else DataHandler._load_ms_info(df=xmls, contents=contents, persons=self.person_names)
        log.info("Loaded MS Info")
        self.text_matrix = texts if texts else DataHandler._load_text_matrix(self.manuscripts)
        log.info("Loaded Text Info")
        self.person_matrix = DataHandler._load_person_matrix(self.manuscripts)
        log.info("Loaded Person-MSS-Matrix Info")
        self.groups = Groups.from_cache() or Groups()
        self.manuscripts.drop(columns=["content", "soup"], inplace=True)
        log.info("Successfully created a Datahandler instance.")
        # FIXME: this indicates that data is up to date, even if cache was used.
        # maybe cache should be disabled if there is a difference in the first place?
        GitUtil.update_handler_state()

    # Static Methods
    # ==============

    @staticmethod
    def _from_pickle() -> Optional[DataHandler]:
        """Load datahandler from pickle, if available. Returns None otherwise."""
        if os.path.exists(HANDLER_PATH_PICKLE):
            try:
                prev = sys.getrecursionlimit()
                with open(HANDLER_PATH_PICKLE, mode='rb') as file:
                    sys.setrecursionlimit(prev * 100)
                    obj = pickle.load(file)
                    sys.setrecursionlimit(prev)
                    if isinstance(obj, DataHandler):
                        # obj._truncate()
                        return obj
            except Exception:
                log.exception("Cound not load handler from pickle")
        return None

    # @staticmethod
    # def _from_backup() -> Optional[DataHandler]:
    #     # QUESTION: does it actually need this? or is one type of backup enough?
    #     # CHORE: document
    #     mss = "data/backups/mss.csv"
    #     txts = "data/backups/txts.csv"
    #     ppls = "data/backups/ppls.csv"
    #     if os.path.exists(mss) and os.path.exists(txts) and os.path.exists(ppls):
    #         m = pd.read_csv(mss)
    #         if m.empty:
    #             return None
    #         t = pd.read_csv(txts)
    #         if t.empty:
    #             return None
    #         p = pd.read_csv(ppls)
    #         if p.empty:
    #             return None
    #         handler = DataHandler(manuscripts=m, texts=t)
    #         handler._truncate()
    #         return handler
    #     return None

    @staticmethod
    def _load_ms_info(persons: Dict[str, str],
                      df: Optional[pd.DataFrame] = None,
                      contents: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        # CHORE: document
        if df is None or contents is None:
            df = tamer.deliver_handler_data()
        df['soup'] = df['content'].apply(lambda x: BeautifulSoup(x, 'xml', from_encoding='utf-8'))
        msinfo = df['soup'].apply(lambda x: tamer.get_msinfo(x, persons))
        log.info("Loaded MS Info")
        df = df.join(msinfo)
        return df

    @staticmethod
    def _load_text_matrix(df: pd.DataFrame) -> pd.DataFrame:
        # CHORE: document
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
        # CHORE: document
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
        # CHORE: document
        person_names = tamer.get_person_names()
        return person_names, tamer.get_person_names_inverse(person_names)

    @staticmethod
    def is_cached() -> bool:
        # CHORE: document
        return os.path.exists(HANDLER_PATH_PICKLE)

    # @staticmethod TODO: Should no longer be needed. If repo is there, data should be there?
    # def has_data_available() -> bool:
    #     # CHORE: document
    #     return tamer.has_data_available()

    # Class Methods
    # =============

    @classmethod
    def get_handler(cls, xmls: Optional[pd.DataFrame] = None, contents: Optional[pd.DataFrame] = None,) -> DataHandler:
        """Get a DataHandler

        Factory method to get a DataHandler object.

        Args:

        Returns:
            DataHandler: A DataHandler, either loaded from cache or created anew.
        """
        log.info("Getting DataHandler")
        res: Optional[DataHandler] = cls._from_pickle()
        if res:
            res._backup()
            return res
        log.info("Could not get DataHandler from pickle")
        # res = cls._from_backup()
        # if res:
        #     res._to_pickle()
        #     return res
        # log.info("Could not get DataHandler from backup")
        res = cls(xmls=xmls, contents=contents)
        res._to_pickle()
        res._backup()
        log.info("DataHandler ready.")
        return res

    # Instance Methods
    # ================

    def _to_pickle(self) -> None:
        log.info("Saving handler to pickle")
        prev = sys.getrecursionlimit()
        with open(HANDLER_PATH_PICKLE, mode='wb') as file:
            try:
                sys.setrecursionlimit(prev * 100)
                pickle.dump(self, file)
                sys.setrecursionlimit(prev)
            except Exception:
                log.exception("Failed to pickle the data handler.")

    def _backup(self) -> None:
        self.manuscripts.to_csv(HANDLER_BACKUP_PATH_MSS, encoding='utf-8', index=False)
        log.info('Backed up manuscripts')
        # self.text_matrix.to_parquet(HANDLER_BACKUP_PATH_TXT_MATRIX)
        # log.info('Backed up text matrix')
        # self.person_matrix.to_csv(HANDLER_BACKUP_PATH_PERS_MATRIX, encoding='utf-8', index=False)
        # log.info('Backed up person matrix')
        with open(HANDLER_BACKUP_PATH_PERS_DICT, encoding='utf-8', mode='w+') as f:
            json.dump(self.person_names, f, ensure_ascii=False, indent=4)
            log.info('Backed up person dict')
        with open(HANDLER_BACKUP_PATH_PERS_DICT_INV, encoding='utf-8', mode='w+') as f:
            json.dump(self.person_names_inverse, f, ensure_ascii=False, indent=4)
            log.info('Backed up inverse person dict')

    # API Methods
    # -----------

    def get_all_manuscript_data(self) -> pd.DataFrame:
        """Get the manuscripts dataframe.

        Returns:
            A dataframe containing all manuscripts with their respective metadata.

            The dataframe will have the followin structure:

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
        return self.manuscripts

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
        mss_ = self.manuscripts[self.manuscripts['shelfmark'].isin(Inmss)]
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
                log.info('no texts fond')
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
        return self.person_names[pers_id]

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

    # TASKS: more handler API
    # - options to work with subcorpora?
    # - add rubrics?
