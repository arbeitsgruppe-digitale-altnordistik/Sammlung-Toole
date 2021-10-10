"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
from bs4 import BeautifulSoup
import pandas as pd
import pickle
import os
import util.tamer as tamer
from util import utils, metadata
from util.constants import HANDLER_PATH_PICKLE, HANDLER_BACKUP_PATH_MSS
from util.utils import Settings, SearchOptions
import numpy as np
from scipy import sparse


log = utils.get_logger(__name__)
settings = Settings.get_settings()


class DataHandler:
    manuscripts: pd.DataFrame
    """Manuscripts
    
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

    person_names: Dict[str, str]
    """Name lookup dictionary

    Lookup dictionary mapping person IDs to the full name of the person
    """

    text_matrix: pd.DataFrame
    """Text-Manuscript-Matrix

    Sparse matrix with a row per manuscript and a column per text name.
    True, if the manuscript contains the text.
    Allows for lookups, which manuscripts a particular text is connected to.
    """

    person_matrix: pd.DataFrame
    """Person-Manuscript-Matrix

    Sparse matrix with a row per manuscript and a column per person ID.
    True, if the manuscript is connected to the person (i.e. the description has the person tagged).
    Allows for lookups, which manuscripts a particular person is connected to.
    """

    subcorpora: List[Any]  # TODO: implement
    # CHORE: document

    def __init__(self,
                 manuscripts: pd.DataFrame = None,
                 texts: pd.DataFrame = None,
                 xmls: Optional[pd.DataFrame] = None,
                 contents: Optional[pd.DataFrame] = None):
        # CHORE: document
        log.info("Creating new handler")
        self.person_names = DataHandler._load_persons()
        log.info("Loaded Person Info")
        self.manuscripts = manuscripts if manuscripts else DataHandler._load_ms_info(df=xmls, contents=contents, persons=self.person_names)
        log.info("Loaded MS Info")
        self.text_matrix = texts if texts else DataHandler._load_text_matrix(self.manuscripts)
        log.info("Loaded Text Info")
        self.person_matrix = DataHandler._load_person_matrix(self.manuscripts)
        log.info("Loaded Person-MSS-Matrix Info")
        self.subcorpora: List[Any] = []  # TODO: implement
        self.manuscripts.drop(columns=["content", "soup"], inplace=True)
        log.info("Successfully created a Datahandler instance.")

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
                        obj._truncate()
                        return obj
            except Exception:
                log.exception("Cound not load handler from pickle")
        return None

    @staticmethod
    def _from_backup() -> Optional[DataHandler]:
        # QUESTION: does it actually need this? or is one type of backup enough?
        # CHORE: document
        mss = "data/backups/mss.csv"
        txts = "data/backups/txts.csv"
        ppls = "data/backups/ppls.csv"
        if os.path.exists(mss) and os.path.exists(txts) and os.path.exists(ppls):
            m = pd.read_csv(mss)
            if m.empty:
                return None
            t = pd.read_csv(txts)
            if t.empty:
                return None
            p = pd.read_csv(ppls)
            if p.empty:
                return None
            handler = DataHandler(manuscripts=m, texts=t)
            handler._truncate()
            return handler
        return None

    @staticmethod
    def _load_ms_info(persons: Dict[str, str],
                      df: Optional[pd.DataFrame] = None,
                      contents: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        # CHORE: document
        if df is None or contents is None:
            df = tamer.deliver_handler_data()
        df['soup'] = df['content'].apply(lambda x: BeautifulSoup(x, 'xml'))
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
    def _load_persons() -> Dict[str, str]:
        # CHORE: document
        if not tamer.has_person_data_available():
            tamer.unzip_person_xmls()
        return tamer.get_person_names()

    @staticmethod
    def is_cached() -> bool:
        # CHORE: document
        return os.path.exists(HANDLER_PATH_PICKLE)

    @staticmethod
    def has_data_available() -> bool:
        # CHORE: document
        return tamer.has_data_available()

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
        if res and res._ms_complete():
            res._backup()
            return res
        log.info("Could not get DataHandler from pickle")
        res = cls._from_backup()
        if res:
            res._to_pickle()
            return res
        log.info("Could not get DataHandler from backup")
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
        # TODO: implement backing up other props to csv/json
        # TODO: do we still want/need this

    def _truncate(self) -> None:
        if len(self.manuscripts.index) > settings.max_res:
            self.manuscripts = self.manuscripts[:settings.max_res]
        # TODO: truncate other props aswell
        # TODO: drop max res entirely?

    def _ms_complete(self) -> bool:
        return True
        # TODO: implement more reasonable solution
        # if self.manuscripts is None or self.manuscripts.empty:
        #     return False
        # length = len(self.manuscripts.index)
        # if length >= settings.max_res:
        #     return True
        # if length >= crawler.crawl_collections()['ms_count'].sum():
        #     return True
        # return False

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
                               filenames: Union[List[str], pd.Series, pd.DataFrame] = None) -> Optional[pd.DataFrame]:
        """Search manuscript metadata for certain manuscripts.

        Basic search function:

        Searches for manuscripts with a certain IDs, and returns the metadata for the respective manuscripts.

        IDs can either be full_id (i.e. a certain catalogue entry),
        ms_ids (i.e. a certain manuscript that can have catalogue entries in multiple languages)
        or filenames (refers to the XML files of the catalogue entry).

        Note: Exactly one of the three optional parameters should be passed.

        Args:
            full_ids (Union[List[str], pd.Series, pd.DataFrame], optional): List/Series/Dataframe of catalogue entry IDs. Defaults to None.
            ms_ids (Union[List[str], pd.Series, pd.DataFrame], optional): List/Series/Dataframe of manuscript IDs. Defaults to None.
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
        if searchOption == SearchOptions.CONTAINS_ONE:
            hits = []
            for t in texts:
                df = self.text_matrix[self.text_matrix[t] == True]
                mss = list(df.index)
                hits += mss
            return list(set(hits))
        else:
            hits = []
            for t in texts:
                df = self.text_matrix[self.text_matrix[t] == True]
                s = set(df.index)
                hits.append(s)
            intersection = set.intersection(*hits)
            return list(intersection)

    def search_texts_contained_by_manuscripts(self, mss: List[str], searchOption: SearchOptions) -> List[str]:
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
        log.info(f'Searching for texts contained by manuscripts: {mss} ({searchOption})')
        df = self.text_matrix.transpose()
        if searchOption == SearchOptions.CONTAINS_ONE:
            hits = []
            for ms in mss:
                d = df[df[ms] == True]
                mss = list(d.index)
                hits += mss
            return list(set(hits))
        else:
            sets = []
            for ms in mss:
                d = df[df[ms] == True]
                s = set(d.index)
                sets.append(s)
            intersection = set.intersection(*sets)
            return list(intersection)

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

    # TASKS: more handler API
    # - more options how to get ms data
    # - options to get texts
    # - options to get persons
    # - options to work with subcorpora?
    # - add rubrics?
