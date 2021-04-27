from typing import Generator, List, Optional, Tuple
from numpy import integer
import pandas as pd
import requests
import os
import glob
from bs4 import BeautifulSoup
from datetime import datetime
import utils
import pickle
from stqdm import stqdm
from threading import Thread
from lxml import etree


log = utils.get_logger(__name__)


# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'
_potential_xml_url_path = 'data/pot_ms_urls.csv'
_xml_url_path = 'data/ms_urls.csv'
_xml_data_prefix = 'data/xml/'
_xml_content_pickle_path = 'data/contents.pickle'
_xml_url_prefix = 'https://handrit.is/en/manuscript/xml/'
PICKLE_PATH = "data/cache.pickle"
BACKUP_PATH_MSS = "data/backups/mss.csv"

verbose = True


# Crawl Collections
# -----------------

def crawl_collections(use_cache: bool = True, cache: bool = True) -> pd.DataFrame:
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
    log.info('Loading Collegtions')
    if use_cache and os.path.exists(_coll_path):
        cols = pd.read_csv(_coll_path)
        if cols is not None and not cols.empty:
            log.info('Loaded collections from cache.')
            return cols
    cols = _load_collections()
    if cache:
        cols.to_csv(_coll_path, encoding='utf-8', index=False)
    log.info(f"Loaded {len(cols.index)} collections.")
    return cols


def _load_collections() -> pd.DataFrame:
    """Load collections from website
    """
    soup = utils.get_soup('https://handrit.is/#collection', 'lxml')
    collection_tags = soup.find_all('div', attrs={'class': 'collection'})
    collections = [(c.find('span', attrs={'class': 'mark'}).text,
                    int(c.find('span', attrs={'class': 'count'}).text.split()[0]),
                    c.find('a', attrs={'class': 'viewall'})['href'].rsplit(';')[0] + '?showall.browser=1',) for c in collection_tags]
    df = pd.DataFrame(collections, columns=['collection', 'ms_count', 'url'])
    return df


# Crawl Manuscript IDs
# --------------------

def crawl_ids(df: pd.DataFrame = None, use_cache: bool = True, cache: bool = True, max_res: int = -1) -> pd.DataFrame:
    """Load all manuscript IDs.

    The dataframe contains the following collumns:
    - Collection ID (`collection`)
    - Manuscript ID (`id`)

    Args:
        df (pd.DataFrame, optional): Dataframe containing the available collections on handrit. If `None` is passed, `get_collections()` will be called. Defaults to None.
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
        max_res (int, optional): Maximum number of results to return (mostly for testing quickly). 
            For unrestricted, use -1. Defaults to -1.

    Returns:
        pd.DataFrame: Dataframe containing the manuscript IDs.
    """
    # LATER: could add progressbar to this one too
    log.info('Loading Manuscript IDs')
    if use_cache and os.path.exists(_id_path):
        ids = pd.read_csv(_id_path)
        if ids is not None and not ids.empty and _is_ids_complete(ids):
            # TODO: improve working with half finished caches (e.g. after max_res)
            log.info('Loaded manuscript IDs from cache.')
            return ids
    if df is None:
        df = crawl_collections(use_cache=use_cache, cache=cache)
    ids = _load_ids(df, max_res=max_res)
    if max_res > 0 and len(ids.index) >= max_res:
        ids = ids[:max_res]
    ids.sort_values(by=['collection', 'id'])
    if cache:
        ids.to_csv(_id_path, encoding='utf-8', index=False)
    log.info(f"Loaded {len(ids.index)} IDs.")
    return ids


def _is_ids_complete(ids: pd.DataFrame) -> bool:
    """indicates if the number of IDs matches the number indicated by the collections"""
    # TODO: improve this
    colls = crawl_collections()
    if colls['ms_count'].sum() == len(ids.index):
        return True
    log.warning('Number of manuscripts does not match the number indicated by collections')
    return False


def _load_ids(df: pd.DataFrame, max_res: int = -1) -> pd.DataFrame:
    """Load IDs"""

    def get_iter(df: pd.DataFrame, max_res: int):
        hits = 0
        cols = list(df.collection)
        if max_res <= 0:
            max_res = float('inf')
        for col in cols:
            if hits >= max_res:
                break
            url = df.loc[df.collection == col, 'url'].values[0]
            for res in _download_ids_from_url(url, col):
                if hits >= max_res:
                    break
                yield res
                hits += 1

    iterator_ = get_iter(df, max_res)
    res = pd.DataFrame(iterator_, columns=['collection', 'id'])
    return res


def _download_ids_from_url(url: str, col: str) -> List[Tuple[str]]:
    """get IDs from a collection URL"""
    soup = utils.get_soup(url, 'lxml')
    res = []
    for td in soup.find_all('td', attrs={'class': 'id'}):
        res.append((col, td.text))
    return res


# Crawl XML URLs
# --------------

def crawl_xmls(df: pd.DataFrame = None, use_cache: bool = True, cache: bool = True, max_res: int = -1, prog=None) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Load all manuscript XMLs.

    The dataframe contains the following collumns:
    - Collection ID (`collection`)
    - Manuscript ID (`id`)
    - language (`lang`)
    - URL to XML (`xml_url`)
    # CHORE: update

    Args:
        df (pd.DataFrame, optional): Dataframe containing the available manuscript IDs. If `None` is passed, `get_ids()` will be called. Defaults to None.
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
        max_res (int, optional): Maximum number of results to return (mostly for testing quickly). For unrestricted, use -1. Defaults to -1.
        prog (streamlit.beta_container, optional): streamlit context for displaying progress bars

    Returns:
        pd.DataFrame: Dataframe containing manuscript URLs.
    """
    if use_cache and os.path.exists(_xml_url_path):
        log.info("Loading XML URLs from cache.")
        xmls = pd.read_csv(_xml_url_path)
        if os.path.exists(_xml_content_pickle_path):
            with open(_xml_content_pickle_path, mode='rb') as file:
                contents = pickle.load(file)
                return xmls, contents
        return xmls, None
        # TODO: get this to work again
        # if res is not None and not res.empty and _is_urls_complete(res):
        # TODO: improve working with half finished caches (e.g. after max_res)
        #    if verbose:
        #         print('Loaded XML URLs from cache.')
        #     return res
    log.info("Loading XML URLs.")
    if df is None:
        df = crawl_ids(df=None, use_cache=use_cache, cache=cache, max_res=max_res)
    if max_res > 0 and max_res < len(df.index):
        df = df[:max_res]
    log.info("Building potential URLs")
    potentials = _get_potential_xmls(df, prog=prog, use_cache=use_cache, cache=cache)
    log.info("Loading actual XMLs")
    xmls = _get_existing_xmls(potentials, prog=prog, use_cache=use_cache, cache=cache, max_res=max_res)
    xmls['shelfmark'] = xmls['content'].apply(_get_shelfmark)
    contents = xmls[['content', 'xml_file']]
    contents['path'] = _xml_data_prefix + contents['xml_file']
    xmls.drop(columns=['content'], inplace=True)
    xmls.drop(columns=['exists'], inplace=True)
    if cache:
        xmls.to_csv(_xml_url_path, encoding='utf-8', index=False)
        with open(_xml_content_pickle_path, mode='wb') as file:
            pickle.dump(contents, file=file)
    return xmls, contents


def _get_existing_xmls(potentials: pd.DataFrame, prog=None, use_cache=True, cache=True, max_res=-1) -> pd.DataFrame:
    if max_res > 0 and len(potentials.index) > (3 * max_res):
        potentials = potentials[:(max_res * 3)]
    with prog:
        stqdm.pandas(desc="Loading XMLs")
        potentials['content'] = potentials.progress_apply(lambda x: _get_xml_content_if_exists(x, cache, use_cache), axis=1)
        # TODO: ensure that prog==None doesn't break anything anywhere
    potentials['exists'] = potentials['content'].apply(lambda x: True if x else False)
    df = potentials[potentials['exists'] == True]
    if max_res > 0 and len(df.index > max_res):
        df = df[:max_res]
    return df


def _get_xml_content_if_exists(row: pd.Series, cache, use_cache):
    path = f"{_xml_data_prefix}{row['xml_file']}"
    if use_cache and os.path.exists(path):
        with open(path, encoding='utf-8', mode='r+') as file:
            return file.read()
    content = _load_xml_content(row['xml_url'])
    if not content:
        return None
    if cache:
        Thread(target=_cache_xml_content, args=(content, path)).start()
    return content


def _get_potential_xmls(id_df: pd.DataFrame, prog=None, use_cache=True, cache=True) -> pd.DataFrame:
    if use_cache and os.path.exists(_potential_xml_url_path):
        log.info("Loading XML URLs from cache.")
        res = pd.read_csv(_potential_xml_url_path)
        return res

    def iterate():
        df = pd.DataFrame(columns=['collection', 'id', 'lang', 'xml_url'])
        for _, row in stqdm(id_df.iterrows(), total=len(id_df.index), desc="Calculating Potential URLs"):
            langs = ('en', 'da', 'is')
            for l in langs:
                file = f'{row[1]}-{l}.xml'
                url = f'{_xml_url_prefix}{file}'
                df = df.append({'collection': row[0],
                                'id': row[1],
                                'lang': l,
                                'xml_url': url,
                                'xml_file': file
                                }, ignore_index=True)
        return df
    if prog:
        with prog:
            df = iterate()
    else:
        df = iterate()
    if cache:
        df.to_csv(_potential_xml_url_path, encoding='utf-8', index=False)
    return df


def _is_urls_complete(df: pd.DataFrame) -> bool:
    # TODO: improve?
    ids_a = len(crawl_ids()['id'].unique())
    ids_b = len(df['id'].unique())
    return ids_a == ids_b


def _cache_xml_content(content, path):
    with open(path, mode='w+', encoding='utf-8') as file:
        file.write(content)


def _load_xml_content(url):
    """Load XML content from URL, ensuring the encoding is correct."""
    response = requests.get(url)
    if response.status_code != 200:
        return None
    bytes_ = response.text.encode(response.encoding)
    if bytes_.startswith(b'<!DOCTYPE html'):
        log.error(f"Content is not XML: {url}")
        return ""
    if bytes_[0] == 255:
        try:
            bytes_ = bytes_[2:]
            txt = bytes_.decode('utf-16le')
            txt = txt.replace('UTF-16', 'UTF-8', 1)
            log.warning(f"Found XML in encoding 'UTF-16-LE'. Converted to 'UTF-8'. Check if data was lost in the process.\nXML: {url}")
            return txt
        except Exception as e:
            log.warning(f"Failed to convert 'UTF-16-LE'. Fall back to 'ISO-8859-1'. Check if data was lost in the process.\nXML: {url}")
            log.exception(e)
            return response.text.replace('iso-8859-1', 'UTF-8')
    else:
        if bytes_.startswith(b'<?xml version="1.0" encoding="UTF-8"') or \
                bytes_.startswith(b'<?xml version="1.0" encoding="utf-8"'):
            try:
                txt = bytes_.decode('utf-8')
                return txt
            except Exception as e:
                log.warning(f"Failed to convert {url}")
                log.exception(e)
                return ""
        elif bytes_.startswith(b'<?xml version="1.0" encoding="iso-8859-1"'):
            try:
                txt = bytes_.decode('iso-8859-1')
                txt = txt.replace('iso-8859-1', 'UTF-8')
                log.info(f"Loaded {url} - Encoding: ISO-8859-1 changed to UTF-8")
                return txt
            except Exception as e:
                log.warning(f"Failed to convert {url}")
                log.exception(e)
                return ""
        else:
            log.warning(f"Unknown encoding: {url} Assuming UTF-8")
            try:
                txt = bytes_.decode('utf-8')
                return '<?xml version="1.0" encoding="UTF-8"?>\n' + txt
            except Exception as e:
                log.warning(f"Failed to convert {url}")
                log.exception(e)
                return ""


def _get_shelfmark(content: str) -> str:
    try:
        root = etree.fromstring(content.encode())
        msDesc = root.find('.//{http://www.tei-c.org/ns/1.0}msDesc')
        msID = msDesc.get('{http://www.w3.org/XML/1998/namespace}id')
        log.debug(f'Shelfmark: {msID}')
        return msID
    except Exception:
        log.exception(f"Faild to load Shelfmark XML:\n\n{content}\n\n")
        return ""


# Access XML Directly
# -------------------

# TODO: rethink this part: is it still the way to go? or is this handler domain?

def load_xml(url: str, use_cache: bool = True, cache: bool = True) -> BeautifulSoup:
    """Load XML from URL.

    Args:
        url (str): URL of an XML on handrit.is.
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.

    Returns:
        BeautifulSoup: XML content.
    """
    filename = url.rsplit('/', 1)[1]
    return load_xml_by_filename(filename=filename, use_cache=use_cache, cache=cache)


def load_xml_by_filename(filename: str, use_cache: bool = True, cache: bool = True) -> BeautifulSoup:
    """Load XML by file name.

    Args:
        filename (str): XML file name.
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.

    Returns:
        BeautifulSoup: XML content.
    """
    path = _xml_data_prefix + filename
    if use_cache and os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return BeautifulSoup(f, 'xml')
    xml = _load_xml_content(_xml_url_prefix + filename)
    if cache:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(xml)
    soup = BeautifulSoup(xml, 'xml')
    return soup


def load_xmls_by_id(id_: str, use_cache: bool = True, cache: bool = True) -> dict:
    """Load XML(s) by manuscript ID.

    Args:
        id_ (str): Manuscript ID
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.

    Returns:
        dict: dict of the XMLs found by this ID. Follows the structure `{language: BeautifulSoup}` (i.e. `{'da': 'soup', 'is': 'soup}`).
    """
    res = {}
    df = crawl_xmls()
    hits = df.loc[df['id'] == id_]
    for _, row in hits.iterrows():
        lang = row['lang']
        url = row['xml_url']
        res[lang] = load_xml(url, use_cache=use_cache, cache=cache)
    return res


def has_data_available() -> bool:
    xmls = glob.glob(_xml_data_prefix + '*.xml')
    return xmls and \
        os.path.exists(_coll_path) and \
        os.path.exists(_id_path) and \
        os.path.exists(_xml_url_path) and \
        os.path.exists(_potential_xml_url_path) and \
        os.path.exists(_xml_content_pickle_path)


def crawl(use_cache: bool = False, prog=None):
    """crawl everything as fast as possible"""
    log.info(f'Start crawling: {datetime.now()}')

    if not use_cache:
        _wipe_cache()
        log.info('Done removing cache.')

    colls = crawl_collections()
    log.info(f"Done loading collections. Found {len(colls.index)} collections containing {colls['ms_count'].sum()} manuscripts.")
    ids = crawl_ids(df=colls)
    log.info(f"Done loading Manuscript IDs. Found {len(ids.index)} unique identifiers.")

    urls, _ = crawl_xmls(df=ids, prog=prog)
    log.info(f"Done loading URLs. Found {len(urls.index)} URLs.")

    log.info(f'Finished: {datetime.now()}')


def _wipe_cache():
    xmls = glob.glob(_xml_data_prefix + '*.xml')
    for xml in xmls:
        os.remove(xml)
    if os.path.exists(_coll_path):
        os.remove(_coll_path)
    if os.path.exists(_id_path):
        os.remove(_id_path)
    if os.path.exists(_xml_url_path):
        os.remove(_xml_url_path)
    if os.path.exists(_potential_xml_url_path):
        os.remove(_potential_xml_url_path)
    if os.path.exists(_xml_content_pickle_path):
        os.remove(_xml_content_pickle_path)
    if os.path.exists(BACKUP_PATH_MSS):
        os.remove(BACKUP_PATH_MSS)
    if os.path.exists(PICKLE_PATH):
        os.remove(PICKLE_PATH)
