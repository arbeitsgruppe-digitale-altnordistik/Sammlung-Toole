"""
This module handles data and provides convenient and efficient access to it.
"""

from __future__ import annotations
from typing import Optional
import pandas as pd
import pickle
import os
import crawler
import handrit_tamer_2 as tamer
import matplotlib.pyplot as plt


class DataHandler:
    def __init__(self,
                 manuscripts: pd.DataFrame = None,
                 texts: pd.DataFrame = None,
                 persons: pd.DataFrame = None):
        print("Creating new handler")
        self.manuscripts = manuscripts if manuscripts else DataHandler._load_ms_info()
        self.texts = texts if texts else pd.DataFrame()  # TODO
        self.persons = persons if persons else pd.DataFrame()  # TODO
        self.subcorpora = []  # TODO: discuss how/what we want

    @staticmethod
    def _from_pickle() -> Optional[DataHandler]:
        if os.path.exists("data/cache.pickle"):
            obj = pickle.load("data/cache.pickle")
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
    def _load_ms_info() -> pd.DataFrame:
        df = crawler.get_xml_urls()
        df['soup'] = df['xml_url'].apply(crawler.load_xml)
        df = df.join(df['soup'].apply(tamer.get_msinfo))
        df.drop(columns=['soup'], inplace=True)  # TODO: here or later? or store soups for quick access?
        return df

    @classmethod
    def get_handler(cls) -> DataHandler:
        """Get a DataHandler

        Factory method to get a DataHandler object.

        Returns:
            DataHandler: A DataHandler, either loaded from cache or created anew.
        """
        print("Getting DataHandler")
        res = cls._from_pickle()
        if res:
            return res
        print("Could not get DataHandler from pickle")
        res = cls._from_backup()
        if res:
            return res
        print("Could not get DataHandler from backup")
        return cls()


if __name__ == "__main__":
    handler = DataHandler.get_handler()
    print("got handler")
    mss = handler.manuscripts
    mss.plot(x='meandate')  # TODO: remove
    plt.show()
    # print(mss.shape)
    # print(mss.head())
    # print(mss['meandate'])
    # print(mss['meandate'].dtypes)
    # print(mss['meandate'][0])
    # print(type(mss['meandate'][0]))
    # conv = mss['meandate'].astype(int)
    # print(conv.dtype)
