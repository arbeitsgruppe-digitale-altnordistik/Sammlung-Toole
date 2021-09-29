"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations
import sys
from typing import Any, List, Optional, Union
from bs4 import BeautifulSoup
import pandas as pd
import pickle
import os
import util.crawler as crawler
import util.tamer as tamer
from util import utils
from util.constants import HANDLER_PATH_PICKLE, HANDLER_BACKUP_PATH_MSS, CRAWLER_PICKLE_PATH
from util.utils import Settings


log = utils.get_logger(__name__)
settings = Settings.get_settings()


class DataHandler:
    def __init__(self,
                 manuscripts: pd.DataFrame = None,
                 texts: pd.DataFrame = None,
                 persons: pd.DataFrame = None,
                 xmls: Optional[pd.DataFrame] = None,
                 contents: Optional[pd.DataFrame] = None):
        """"""  # CHORE: documentation
        log.info("Creating new handler")
        self.manuscripts = manuscripts if manuscripts else DataHandler._load_ms_info(df=xmls, contents=contents)
        log.info("Loaded MS Info")
        self.texts = texts if texts else pd.DataFrame()  # TODO: implement
        self.persons = persons if persons else pd.DataFrame()  # TODO: implement
        self.subcorpora: List[Any] = []  # TODO: implement
        self.manuscripts.drop(columns=["content", "soup"], inplace=True)

    # Static Methods
    # ==============

    @staticmethod
    def _from_pickle() -> Optional[DataHandler]:
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
            handler = DataHandler(manuscripts=m, texts=t, persons=p)
            handler._truncate()
            return handler
        return None

    @staticmethod
    def _load_ms_info(df: Optional[pd.DataFrame] = None,
                      contents: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if df is None or contents is None:
            df = tamer.deliver_handler_data()
        # if len(df.index) > settings.max_res:
        #     df = df[:settings.max_res]
        df['soup'] = df['content'].apply(lambda x: BeautifulSoup(x, 'xml'))
        msinfo = df['soup'].apply(tamer.get_msinfo)
        log.info("Loaded MS Info")
        df = df.join(msinfo)
        return df

    @staticmethod
    def is_cached() -> bool:
        return os.path.exists(HANDLER_PATH_PICKLE)

    @staticmethod
    def has_data_available() -> bool:
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

    def _truncate(self) -> None:
        if len(self.manuscripts.index) > settings.max_res:
            self.manuscripts = self.manuscripts[:settings.max_res]
        # TODO: truncate other props aswell

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
        # CHORE: documentation
        return self.manuscripts

    def get_manuscript_data(self,
                            ids: Union[List[str], pd.Series, pd.DataFrame] = None,
                            urls: Union[List[str], pd.Series, pd.DataFrame] = None,
                            filenames: Union[List[str], pd.Series, pd.DataFrame] = None) -> pd.DataFrame:
        # CHORE: documentation: one of these arguments must be passed, return df to mss
        pass  # TODO: implement

    def get_ms_urls_from_search_or_browse_urls(self, urls: List[str], sharedMode: bool = False) -> List[str]:
        # CHORE: documentation
        msss: List[pd.DataFrame] = []
        for url in urls:
            if "/search/results/" in url:
                pages = tamer.get_search_result_pages(url)
                shelfmarks = tamer.get_shelfmarks_from_urls(pages)
                print(shelfmarks)
                mss = self.manuscripts[self.manuscripts['shelfmark'].isin(shelfmarks)]
            else:
                ids = tamer.efnisordResult(url)
                mss = self.manuscripts[self.manuscripts['id'].isin(ids)]
            msss.append(mss)

        if sharedMode:  # Don't these two return the exact same result?! Shared was for textworks...
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
