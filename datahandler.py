"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations
from typing import Any, List, Optional, Union
from bs4 import BeautifulSoup
import pandas as pd
import pickle
import os
import crawler
import handrit_tamer_2 as tamer
from util import utils
from util.constants import HANDLER_PATH_PICKLE, HANDLER_BACKUP_PATH_MSS, CRAWLER_PICKLE_PATH
from stqdm import stqdm


log = utils.get_logger(__name__)


class DataHandler:
    def __init__(self, manuscripts: pd.DataFrame = None, texts: pd.DataFrame = None, persons: pd.DataFrame = None, max_res: int = -1, prog: Any = None):
        """"""  # CHORE: documentation
        log.info("Creating new handler")
        self.manuscripts = manuscripts if manuscripts else DataHandler._load_ms_info(max_res=max_res, prog=prog)
        self.texts = texts if texts else pd.DataFrame()  # TODO
        self.persons = persons if persons else pd.DataFrame()  # TODO
        self.subcorpora: List[Any] = []  # TODO: discuss how/what we want

    # Static Methods
    # ==============

    @staticmethod
    def _from_pickle() -> Optional[DataHandler]:
        if os.path.exists(HANDLER_PATH_PICKLE):
            with open(HANDLER_PATH_PICKLE, mode='rb') as file:
                obj = pickle.load(file)
                if isinstance(obj, DataHandler):
                    return obj
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
            return DataHandler(manuscripts=m, texts=t, persons=p)
        return None

    @staticmethod
    def _load_ms_info(max_res: int = -1, prog: Any = None) -> pd.DataFrame:
        df, contents = crawler.crawl_xmls(max_res=max_res)
        if max_res > 0 and len(df.index) > max_res:
            df = df[:max_res]
        df = pd.merge(df, contents, on='xml_file')
        stqdm.pandas(desc="Cooking soups from XML contents...")
        if prog:
            with prog:
                df['soup'] = df['content'].progress_apply(lambda x: BeautifulSoup(x, 'xml'))
        else:
            df['soup'] = df['content'].progress_apply(lambda x: BeautifulSoup(x, 'xml'))
        stqdm.pandas(desc="Boiling soups down to the essence of metadata...")
        if prog:
            with prog:
                msinfo = df['soup'].progress_apply(tamer.get_msinfo)
        else:
            msinfo = df['soup'].apply(tamer.get_msinfo)
        df = df.join(msinfo)
        # df.drop(columns=['soup'], inplace=True)  # TODO: here or later? or store soups for quick access?
        return df

    @staticmethod
    def is_cached() -> bool:
        return os.path.exists(HANDLER_PATH_PICKLE)

    @staticmethod
    def has_data_available() -> bool:
        return crawler.has_data_available()

    # Class Methods
    # =============

    @classmethod
    def get_handler(cls, max_res: int = -1, prog: Any = None) -> DataHandler:
        """Get a DataHandler

        Factory method to get a DataHandler object.

        Args:
            max_res (int, optional): maximum number of results, mostly for testing purposes. Defaults to -1.
            # CHORE: prog

        Returns:
            DataHandler: A DataHandler, either loaded from cache or created anew.
        """
        log.info("Getting DataHandler")
        res: Optional[DataHandler] = cls._from_pickle()  # TODO: max_res
        if res:
            res._backup()
            return res
        log.info("Could not get DataHandler from pickle")
        res = cls._from_backup()  # TODO: max_res
        if res:
            res._to_pickle()
            return res
        log.info("Could not get DataHandler from backup")
        res = cls(max_res=max_res, prog=prog)
        res._to_pickle()
        res._backup()
        return res

    # Instance Methods
    # ================

    def _to_pickle(self) -> None:
        with open(HANDLER_PATH_PICKLE, mode='wb') as file:
            pickle.dump(self, file)

    def _backup(self) -> None:
        self.manuscripts.to_csv(HANDLER_BACKUP_PATH_MSS, encoding='utf-8', index=False)
        # TODO: implement rest to csv/json

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

    # TODO: more API
    # - more options how to get ms data
    # - options to get texts
    # - options to get persons
    # - options to work with subcorpora?

    # TODO: even more stuff:
    # - logging
    # - improve crawler
    # - tidy up everything
    # - ...
    #
    # .
