from typing import List
from numpy import empty
import pandas as pd
import requests
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from time import time


# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'
_xml_url_path = 'data/xml_urls.csv'


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
    """Load all collections from handrit.isself.

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
# -----------------

def get_ids(urls: List[str], use_cache: bool=True, cache: bool=True, max_res: int=-1, aggressive_crawl: bool=True) -> List[str]:
    # TODO: Docstring
    # print(*urls, sep='\n')
    if use_cache and os.path.exists(_id_path):
        ids = pd.read_csv(_id_path)
        if ids is not None and not ids.empty:
            return list(ids.id)
    ids = _load_ids(urls, max_res=max_res, aggressive_crawl=aggressive_crawl)
    if max_res > 0 and len(ids) >= max_res:
        ids = ids[:max_res]
    if cache:
        df = pd.DataFrame({'id': ids})
        df.to_csv(_id_path, encoding='utf-8', index=False)
    return list(ids)


def _load_ids(urls: List[str], max_res: int=-1, aggressive_crawl: bool=False) -> pd.DataFrame:
    if aggressive_crawl:
        with ThreadPoolExecutor() as executor:
            res = executor.map(_load_ids_from_url, urls)
        return list(set(chain.from_iterable(res)))
    else:
        res = []
        for url in urls:
            res.extend(_load_ids_from_url(url))
            if max_res > 0 and len(res) >= max_res:
                break
        return list(set(res))


def _load_ids_from_url(url):
    soup = _get_soup(url, 'lxml')
    res = []
    for td in soup.find_all('td', attrs={'class': 'id'}):
        res.append(td.text)
    return res
    

# Crawl Manuscript IDs
# -----------------

def get_xml_urls(ids: List[str], use_cache: bool=True, cache: bool=True, max_res: int=-1, aggressive_crawl: bool=True) -> List[str]:
    # TODO: Docstring
    if use_cache and os.path.exists(_xml_url_path):
        ids = pd.read_csv(_xml_url_path)
        if ids is not None and not ids.empty:
            return list(ids.url)
    potential_urls = _get_potential_xml_urls(ids)
    existing_urls = _get_existing_xml_urls(potential_urls, aggressive_crawl)  # TODO: implement
    res = []
    for i, url in enumerate(existing_urls):
        if max_res > 0 and i >= max_res:
            break
        res.append(url)
        print(url)
    if cache:
        df = pd.DataFrame({'url': res})
        df.to_csv(_xml_url_path, encoding='utf-8', index=False)
    return res


def _get_potential_xml_urls(ids):
    pref = 'https://handrit.is/en/manuscript/xml/'
    return chain.from_iterable((f'{pref}{id}-en.xml', f'{pref}{id}-da.xml', f'{pref}{id}-is.xml') for id in ids)


def _get_existing_xml_urls(potentials, aggressive):
    if aggressive:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(_get_url_if_exists, p) for p in potentials]
            for f in as_completed(futures):
                res = f.result()
                if res:
                    yield res
    else:
        for pot in potentials:
            if _get_url_if_exists(pot):
                yield pot


def _get_url_if_exists(url):
    status = requests.get(url).status_code
    if status == 200:
        return url


# Test Runner
# -----------

if __name__ == "__main__":
    cols = get_collections()
    # print(cols)
    ids = get_ids(list(cols.url))
    start = time()
    xml_urls = get_xml_urls(ids, aggressive_crawl=True, use_cache=False, max_res=50)
    stop = time()
    print(stop - start)
