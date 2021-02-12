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
_xml_url_path = 'data/ms_urls.csv'

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

def get_collections(use_cache: bool = True, cache: bool = True) -> pd.DataFrame:
    """Load all collections from handrit.is.

    The dataframe contains the following informations:
    - Collection ID (`collection`)
    - Number of Manuscripts listed for the Collection (`ms_count`)
    - Collection URL (`url`)

    Args:
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.

    Returns:
        pd.DataFrame: Data frame containing basic information on collections.
    """
    if use_cache and os.path.exists(_coll_path):
        cols = pd.read_csv(_coll_path)
        if cols is not None and not cols.empty:
            print('Loaded collections from cache.')
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
                    int(c.find('span', attrs={
                        'class': 'count'}).text.split()[0]),
                    c.find('a', attrs={'class': 'viewall'})['href'].rsplit(';')[0] + '?showall.browser=1',) for c in collection_tags]
    df = pd.DataFrame(collections, columns=['collection', 'ms_count', 'url'])
    return df


# Crawl Manuscript IDs
# --------------------

def get_ids(df: pd.DataFrame, use_cache: bool = True, cache: bool = True, max_res: int = -1, aggressive_crawl: bool = True) -> pd.DataFrame:
    """Load all manuscript IDs.

    The dataframe contains the following collumns:
    - Collection ID (`collection`)
    - Manuscript ID (`id`)

    Args:
        df (pd.DataFrame): Dataframe containing the available collections on handrit. (Cf. `get_collections()`)
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
        max_res (int, optional): Maximum number of results to return (mostly for testing quickly). 
            For unrestricted, use -1. Defaults to -1.
        aggressive_crawl (bool, optional): Aggressive crawling mode; not much difference in this instance:
            A bit slower, if a small `max_res` value is chosen; faster, if no `max_res` is set. Defaults to True.

    Returns:
        pd.DataFrame: Dataframe containing the manuscript IDs.
    """
    if use_cache and os.path.exists(_id_path):
        ids = pd.read_csv(_id_path)
        if ids is not None and not ids.empty:
            print('Loaded manuscript IDs from cache.')
            return ids
    ids = _load_ids(df, max_res=max_res, aggressive_crawl=aggressive_crawl)
    if max_res > 0 and len(ids.index) >= max_res:
        ids = ids[:max_res]
    if cache:
        ids.to_csv(_id_path, encoding='utf-8', index=False)
    return ids


def _load_ids(df: pd.DataFrame, max_res: int = -1, aggressive_crawl: bool = True) -> pd.DataFrame:
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


def _load_ids_aggressively(df: pd.DataFrame, max_res: int = -1):
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
        futures = [executor.submit(
            _download_ids_from_url, s.url, s.collection) for _, s in df.iterrows()]
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
        url = df.loc[df.collection == col, 'url'].values[0]
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

def get_xml_urls(df: pd.DataFrame, use_cache: bool = True, cache: bool = True, max_res: int = -1, aggressive_crawl: bool = True) -> pd.DataFrame:
    """Load all manuscript URLsself.

    The dataframe contains the following collumns:
    - Collection ID (`collection`)
    - Manuscript ID (`id`)
    - language (`lang`)
    - URL to XML (`xml_url`)

    Args:
        df (pd.DataFrame): Dataframe containing the available manuscript IDs. (Cf. `get_ids()`)
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
        max_res (int, optional): Maximum number of results to return (mostly for testing quickly). For unrestricted, use -1. Defaults to -1.
        aggressive_crawl (bool, optional): Aggressive crawling mode puts some strain on the server (and your bandwidth) but is much faster. Defaults to True.

    Returns:
        pd.DataFrame: Dataframe containing manuscript URLs.
    """
    if use_cache and os.path.exists(_xml_url_path):
        res = pd.read_csv(_xml_url_path)
        if res is not None and not res.empty:
            print('Loaded XML URLs from cache.')
            return res
    if max_res > 0 and max_res < len(df.index):
        df = df[:max_res]
    potential_urls = df.apply(_get_potential_xml_urls, axis=1)
    existing_urls = _get_existing_xml_urls(potential_urls, aggressive_crawl, max_res)
    if cache:
        existing_urls.to_csv(_xml_url_path, encoding='utf-8', index=False)
    return existing_urls


def _get_potential_xml_urls(row: pd.Series):
    """Create dataframe with all possible xml URLs"""
    id_ = row.id
    pref = 'https://handrit.is/en/manuscript/xml/'
    row['en'] = f'{pref}{id_}-en.xml'
    row['da'] = f'{pref}{id_}-da.xml'
    row['is'] = f'{pref}{id_}-is.xml'
    return row


def _get_existing_xml_urls(potentials: pd.DataFrame, aggressive, max_res) -> pd.DataFrame:
    """Create a dataframe with all URLs that exist (return HTTP code 200) - delegator method."""
    if aggressive:
        iter_ = _get_existing_xml_urls_aggressively(potentials, max_res)
    else:
        iter_ = _get_existing_xml_urls_chillfully(potentials, max_res)
    if max_res > 0:
        res = pd.DataFrame(columns=['collection', 'id', 'lang', 'xml_url'])
        for tuple_ in iter_:
            if len(res.index) >= max_res:
                break
            res = res.append({
                'collection': tuple_[0],
                'id': tuple_[1],
                'lang': tuple_[2],
                'xml_url': tuple_[3],
            }, ignore_index=True)
    else:
        res = pd.DataFrame(iter_, columns=['collection', 'id', 'lang', 'xml_url'])
    print('')
    return res


def _get_existing_xml_urls_chillfully(potentials: pd.DataFrame, max_res) -> Generator[Tuple[str], None, None]:
    """Generator of slowly loaded rows for ms_urls dataframe."""
    if len(potentials.index) > max_res:
        potentials = potentials[:max_res]
    hits = 0
    for checked, row in potentials.iterrows():
        col = row['collection']
        id_ = row['id']
        lang = ['en', 'da', 'is']
        for l in lang:
            res = _get_url_if_exists(col, id_, l, row[l])
            if res:
                hits += 1
                yield res
                percents = hits / max_res * 100
                print(f'Checked {checked+1} \tFound {hits} of {max_res} ({percents:.2f}%)', end=_backspace_print)



def _get_existing_xml_urls_aggressively(potentials: pd.DataFrame, max_res) -> Generator[Tuple[str], None, None]:
    """Generator of multi-thread loaded rows for ms_urls dataframe."""
    options = _get_aggressive_options(potentials, max_res)
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(_get_url_if_exists, *o) for o in options]
        if max_res > 0:
            i = 0
            for f in as_completed(futures):
                if i >= max_res:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                res = f.result()
                if res:
                    i += 1
                    yield res
                    print(f'Found {i}', end=_backspace_print)
        else:
            for i, f in enumerate(as_completed(futures)):
                res = f.result()
                if res:
                    yield res
                    print(f'Found {i}', end=_backspace_print)


def _get_aggressive_options(potentials: pd.DataFrame, max_res) -> Generator[Tuple[str], None, None]:
    """Get linear generator of tuples with potentially existing URLs"""
    if len(potentials.index) > max_res:
        potentials = potentials[:max_res]
    for _, row in potentials.iterrows():
        col = row['collection']
        id_ = row['id']
        lang = ['en', 'da', 'is']
        for l in lang:
            yield col, id_, l, row[l]

def _get_url_if_exists(col, id_, l, url):
    """Returns tuple, if URL returns 200, None otherwise."""
    status = requests.head(url).status_code
    if status == 200:
        return col, id_, l, url


# Test Runner
# -----------

if __name__ == "__main__":
    print(f'Start: {datetime.now()}')
    cols = get_collections()
    ids = get_ids(cols)
    start = time()
    xml_urls = get_xml_urls(ids, use_cache=True)
    # print(xml_urls)
    stop = time()
    print(stop - start)
    print(f'Finished: {datetime.now()}')
