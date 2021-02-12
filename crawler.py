from typing import Generator, List, Tuple
from numpy import empty
import pandas as pd
import requests
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from time import time
from datetime import datetime


# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'
_xml_url_path = 'data/xml_urls.csv'

_backspace_print = '                                     \r'


# Utlity Functions
# ----------------

def _get_soup(url: str, parser='xml') -> BeautifulSoup:
    """Get a BeautifulSoup object from a URL

    Args:
        url (str): The URL
        parser (str, optional): Parser; for HTML, use 'lxml'. Defaults to 'xml'.

    Returns:
        BeautifulSoup: BeautifulSoup object representation of the HTML/XML page.
    """
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, parser)
    return soup
    

# Crawl Collections
# -----------------

def get_collections(use_cache: bool=True, cache: bool=True) -> pd.DataFrame:
    """Load all collections from handrit.is.

    The dataframe contains the following informations:
    - Collection ID
    - Number of Manuscripts listed for the Collection
    - Collection URL

    Args:
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.

    Returns:
        pd.DataFrame: Data frame containing basic information on collections.
    """
    if use_cache and os.path.exists(_coll_path):
        cols = pd.read_csv(_coll_path)
        if cols is not None and not cols.empty:
            return cols
    cols = _load_collections()
    if cache:
        cols.to_csv(_coll_path, encoding='utf-8', index=False)
    return cols


def _load_collections() -> pd.DataFrame:
    """Load collections from website
    """
    soup = _get_soup('https://handrit.is/#collection', 'lxml')
    collection_tags = soup.find_all('div', attrs={'class': 'collection'})
    collections = [(c.find('span', attrs={'class': 'mark'}).text,
                    int(c.find('span', attrs={'class': 'count'}).text.split()[0]),
                    c.find('a', attrs={'class': 'viewall'})['href'].rsplit(';')[0] + '?showall.browser=1',) for c in collection_tags]
    df = pd.DataFrame(collections, columns=['collection', 'ms_count', 'url'])
    return df
    

# Crawl Manuscript IDs
# --------------------

def get_ids(df: pd.DataFrame, use_cache: bool=True, cache: bool=True, max_res: int=-1, aggressive_crawl: bool=True) -> pd.DataFrame:
    """Load all manuscript IDs.

    The dataframe contains the following collumns:
    - Collection ID
    - Manuscript ID

    Args:
        df (pd.DataFrame): Dataframe containing the available collections on handrit. (Cf. `get_collections()`)
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
        max_res (int, optional): Maximum number of results to return (mostly for testing quickly). For unrestricted, use -1. Defaults to -1.
        aggressive_crawl (bool, optional): Aggressive crawlin mode puts some strain on the server (and your bandwidth) but is much faster. Defaults to True.

    Returns:
        pd.DataFrame: Tataframe containing the manuscript IDs.
    """
    if use_cache and os.path.exists(_id_path):
        ids = pd.read_csv(_id_path)
        if ids is not None and not ids.empty:
            return list(ids.id)
    ids = _load_ids(df, max_res=max_res, aggressive_crawl=aggressive_crawl)
    if max_res > 0 and len(ids.index) >= max_res:
        ids = ids[:max_res]
    if cache:
        ids.to_csv(_id_path, encoding='utf-8', index=False)
    return ids


def _load_ids(df: pd.DataFrame, max_res: int=-1, aggressive_crawl: bool=True) -> pd.DataFrame:
    """Load IDs"""
    if aggressive_crawl:
        return _load_ids_aggressively(df, max_res)
    else:
        iterator_ = _load_ids_chillfully(df)
        if max_res > 0:
            res = pd.DataFrame(columns=['collection', 'id'])
            for tuple_ in iterator_:
                if len(res.index) >= max_res:
                    return res
                res = res.append({
                    'collection': tuple_[0],
                    'id': tuple_[1],
                }, ignore_index=True)
        else:
            res = pd.DataFrame(iterator_, columns=['collection', 'id'])
        return res


def _load_ids_aggressively(df: pd.DataFrame, max_res: int=-1):
    """load IDs aggressively"""
    iterator_ = _download_ids_aggressively(df, max_res)
    if max_res > 0:
        res = pd.DataFrame(columns=['collection', 'id'])
        for tuple_ in iterator_:
            if len(res.index) >= max_res:
                return res
            res = res.append({
                'collection': tuple_[0],
                'id': tuple_[1],
            }, ignore_index=True)
    else:
        res = pd.DataFrame(iterator_, columns=['collection', 'id'])
    return res.sort_values(by=['collection', 'id'], inplace=False)



def _download_ids_aggressively(df: pd.DataFrame, max_res: int) -> Generator[Tuple[str], None, None]:
    """Download IDs aggressively: launch Threads"""
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(_download_ids_from_url, s.url, s.collection) for _, s in df.iterrows()]
        i = 0
        for f in as_completed(futures):
            for tup in f.result():
                if max_res > 0 and i >= max_res:
                    executor.shutdown(wait=False, cancel_futures=True)
                    return
                print(i, end='\r')
                i += 1
                yield tup
        print('', end=_backspace_print)


def _load_ids_chillfully(df: pd.DataFrame) -> Generator[Tuple[str], None, None]:
    """Load IDs page by page"""
    cols = list(df.collection)
    for col in cols:
        print(f'loading: {col}', end=_backspace_print)
        url = df.loc[df.collection==col, 'url'].values[0]
        for res in _download_ids_from_url(url, col):
            yield res
    print('', end=_backspace_print)


def _download_ids_from_url(url: str, col: str) -> List[Tuple[str]]:
    """get IDs from a collection URL"""
    soup = _get_soup(url, 'lxml')
    res = []
    for td in soup.find_all('td', attrs={'class': 'id'}):
        res.append((col, td.text))
    return res
    

# Crawl XML URLs
# --------------

def get_xml_urls(ids: List[str], use_cache: bool=True, cache: bool=True, max_res: int=-1, aggressive_crawl: bool=True) -> List[str]:
    # TODO: Docstring
    if use_cache and os.path.exists(_xml_url_path):
        ids = pd.read_csv(_xml_url_path)
        if ids is not None and not ids.empty:
            return list(ids.url)
    potential_urls = _get_potential_xml_urls(ids)
    existing_urls = _get_existing_xml_urls(potential_urls, aggressive_crawl, max_res)  # TODO: implement
    res = []
    for i, url in enumerate(existing_urls):
        if max_res > 0 and i >= max_res:
            break
        res.append(url)
        print(len(res), end='\r')
    if cache:
        df = pd.DataFrame({'url': res})
        df.to_csv(_xml_url_path, encoding='utf-8', index=False)
    return res


def _get_potential_xml_urls(ids):
    pref = 'https://handrit.is/en/manuscript/xml/'
    return chain.from_iterable((f'{pref}{id}-en.xml', f'{pref}{id}-da.xml', f'{pref}{id}-is.xml') for id in ids)


def _get_existing_xml_urls(potentials, aggressive, max):
    if aggressive:
        for pot in _get_existing_xml_urls_aggressively(potentials, max):
            if pot:
                yield pot
    else:
        for pot in potentials:
            if _get_url_if_exists(pot):
                yield pot


def _get_existing_xml_urls_aggressively(potentials, max):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(_get_url_if_exists, p) for p in potentials]
        if max > 0:
            i = 0
            for f in as_completed(futures):
                if i >= max:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                res = f.result()
                if res:
                    i += 1
                    yield res
        else:
            for f in as_completed(futures):
                yield f.result()


def _get_url_if_exists(url):
    status = requests.head(url).status_code
    if status == 200:
        return url


# Test Runner
# -----------

if __name__ == "__main__":
    print(f'Start: {datetime.now()}')
    start = time()
    cols = get_collections()
    ids = get_ids(cols, use_cache=False)
    stop = time()
    # xml_urls = get_xml_urls(ids, use_cache=False)
    print(stop - start)
    print(f'Finished: {datetime.now()}')
