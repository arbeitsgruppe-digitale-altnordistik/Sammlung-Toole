from typing import Generator, List, Optional, Tuple
import pandas as pd
import requests
import os
import glob
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import utils
import guiUtils
import pickle


log = utils.get_logger(__name__)


# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'
_xml_url_path = 'data/ms_urls.csv'
_shelfmark_path = 'data/ms_shelfmarks.csv'
_xml_data_prefix = 'data/xml/'
_soup_pickle_path = 'data/soup.pickle'
_xml_url_prefix = 'https://handrit.is/en/manuscript/xml/'

# _backspace_print = '                                     \r'

verbose = True

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
    log.debug(f'Requesting ({parser}): {url}')
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
    log.info('Loading Collegtions')
    if use_cache and os.path.exists(_coll_path):
        cols = pd.read_csv(_coll_path)
        if cols is not None and not cols.empty:
            log.debug('Loaded collections from cache.')
            return cols
    cols = _load_collections()
    if cache:
        cols.to_csv(_coll_path, encoding='utf-8', index=False)
    log.info(f"Loaded {len(cols.index)} collections.")
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

def get_ids(df: pd.DataFrame = None, use_cache: bool = True, cache: bool = True, max_res: int = -1, prog: guiUtils.Progress = None) -> pd.DataFrame:
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
        # CHORE: prog

    Returns:
        pd.DataFrame: Dataframe containing the manuscript IDs.
    """
    log.info('Loading Manuscript IDs')
    if use_cache and os.path.exists(_id_path):
        ids = pd.read_csv(_id_path)
        if ids is not None and not ids.empty and _is_ids_complete(ids):  # TODO: should work from that, in case of partially finished loading
            log.debug('Loaded manuscript IDs from cache.')
            if prog:
                prog.done()
            return ids
    if df is None:
        df = get_collections(use_cache=use_cache, cache=cache)
    ids = _load_ids(df, max_res=max_res, prog=prog)
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
    colls = get_collections()
    if colls['ms_count'].sum() == len(ids.index):
        log.debug("Number of manuscript IDs matches the number indicated by collections")
        return True
    log.warning('Number of manuscripts does not match the number indicated by collections')
    return False


def _load_ids(df: pd.DataFrame, max_res: int = -1, prog: guiUtils.Progress = None) -> pd.DataFrame:
    """Load IDs"""
    if prog:
        max_count = df['ms_count'].sum()
        if max_res > 0:
            max_count = min(max_count, max_res)
        prog.set_steps(max_count)
    iterator_ = _load_ids_chillfully(df, prog=prog)
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


def _load_ids_chillfully(df: pd.DataFrame, prog: guiUtils.Progress = None) -> Generator[Tuple[str], None, None]:
    """Load IDs page by page"""
    cols = list(df.collection)
    for col in cols:
        log.debug(f'loading: {col}')
        url = df.loc[df.collection == col, 'url'].values[0]
        for res in _download_ids_from_url(url, col):
            if prog:
                prog.increment()
            yield res


def _download_ids_from_url(url: str, col: str) -> List[Tuple[str]]:
    """get IDs from a collection URL"""
    soup = _get_soup(url, 'lxml')
    res = []
    for td in soup.find_all('td', attrs={'class': 'id'}):
        res.append((col, td.text))
    return res


# Crawl XML URLs
# --------------
# TODO: rename. it's not only urls now
def get_xml_urls(df: pd.DataFrame = None, use_cache: bool = True, cache: bool = True, max_res: int = -1, prog: guiUtils.Progress = None) -> pd.DataFrame:
    """Load all manuscript URLs.

    The dataframe contains the following collumns:
    - Collection ID (`collection`)
    - Manuscript ID (`id`)
    - language (`lang`)
    - URL to XML (`xml_url`)

    Args:
        df (pd.DataFrame, optional): Dataframe containing the available manuscript IDs. If `None` is passed, `get_ids()` will be called. Defaults to None.
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
        max_res (int, optional): Maximum number of results to return (mostly for testing quickly). For unrestricted, use -1. Defaults to -1.
        # CHORE: prog

    Returns:
        pd.DataFrame: Dataframe containing manuscript URLs.
    """
    if use_cache and os.path.exists(_xml_url_path):
        log.debug("Loading XML URLs from cache.")
        res = pd.read_csv(_xml_url_path)
        # FIXME: get this to work again
        return res
        # if res is not None and not res.empty and _is_urls_complete(res):  # LATER: should work from that, in case of partially finished loading
        #    if verbose:
        #         print('Loaded XML URLs from cache.')
        #     return res
    log.info("Loading XML URLs.")
    if df is None:
        df = get_ids(df=None, use_cache=use_cache, cache=cache, max_res=max_res)
    if max_res > 0 and max_res < len(df.index):
        df = df[:max_res]
    potential_urls = df.apply(_get_potential_xml_urls, axis=1)  # TODO: improve this with vectorized string operation?
    xmls = _get_existing_xml_urls(potential_urls, max_res, prog, cache).sort_values(by=['collection', 'id'])
    xmls['shelfmark'] = xmls['soup'].apply(_get_shelfmark)
    soups = xmls[['soup', 'xml_file']]
    soups['path'] = _xml_data_prefix + soups['xml_file']
    xmls.drop(columns=['soup'], inplace=True)
    xmls.drop(columns=['content'], inplace=True)
    if cache:
        xmls.to_csv(_xml_url_path, encoding='utf-8', index=False)
        with open(_soup_pickle_path, mode='wb') as file:
            pickle.dump(soups, file=file)
    log.info(f"Loaded {len(xmls.index)} XML URLs.")
    return xmls  # QUESTION: soups too?


def _is_urls_complete(df: pd.DataFrame) -> bool:  # TODO: remove?
    ids_a = len(get_ids()['id'].unique())
    ids_b = len(df['id'].unique())
    return ids_a == ids_b


def _get_potential_xml_urls(row: pd.Series):
    """Create dataframe with all possible xml URLs"""
    id_ = row.id
    row['en'] = f'{_xml_url_prefix}{id_}-en.xml'
    row['da'] = f'{_xml_url_prefix}{id_}-da.xml'
    row['is'] = f'{_xml_url_prefix}{id_}-is.xml'
    return row


def _get_existing_xml_urls(potentials: pd.DataFrame, max_res, prog: guiUtils.Progress = None, cache=True) -> pd.DataFrame:
    """Create a dataframe with all URLs that exist (return HTTP code 200) - delegator method."""  # TODO: update eventually
    if prog:
        max = len(potentials.index)
        if max_res > 0:
            max = max_res * 3 + 2
        prog.set_steps(max)
    iter_ = _get_existing_xml_urls_chillfully(potentials, max_res, prog, cache)
    if max_res > 0:
        res = pd.DataFrame(columns=['collection', 'id', 'lang', 'xml_url', 'xml_file', 'soup'])
        # TODO: add graceful shutdown
        for tuple_ in iter_:
            if len(res.index) >= max_res:
                if prog:
                    prog.done()
                break
            res = res.append({
                'collection': tuple_[0],
                'id': tuple_[1],
                'lang': tuple_[2],
                'xml_url': tuple_[3],
                'xml_file': tuple_[4],
                'soup': tuple_[5],
            }, ignore_index=True)
    else:
        res = pd.DataFrame(iter_, columns=['collection', 'id', 'lang', 'xml_url', 'xml_file', 'soup'])
    return res


def _get_existing_xml_urls_chillfully(potentials: pd.DataFrame, max_res, prog: guiUtils.Progress, cache: bool) -> Generator[Tuple[str], None, None]:
    """Generator of slowly loaded rows for ms_urls dataframe."""
    if max_res > 0 and len(potentials.index) > max_res:
        potentials = potentials[:max_res]
    for _, row in potentials.iterrows():
        col = row['collection']
        id_ = row['id']
        lang = ['en', 'da', 'is']
        for l in lang:
            res = _get_xml_if_exists(col, id_, l, row[l], cache)
            if res:
                if prog:
                    prog.increment()
                yield res


def _get_xml_if_exists(col, id_, l, url: str, cache: bool):
    """Returns tuple, if URL returns 200, None otherwise."""
    file = url.rsplit('/', 1)[1]
    content = _load_xml_content(url)
    if content:
        if cache:
            _cache_xml(file, content)  # XXX: make thread
        return col, id_, l, url, file, BeautifulSoup(content, 'xml')  # XXX: move soup parsing to somewhere else?
    else:
        return False


# Cache XML data
# --------------

def _cache_xml(file, data):
    path = _xml_data_prefix + file
    with open(path, mode='w+', encoding='utf-8') as file:
        file.write(data)

# def cache_all_xml_data(df: pd.DataFrame = None, use_cache: bool = True, cache: bool = True, max_res: int = -1) -> int:
#     """Download all XML data.

#     Args:
#         df (pd.DataFrame, optional): Dataframe containing the existing URLs. If `None` is passed, `get_xml_urls()` will be called. Defaults to None.
#         use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
#         cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
#         max_res (int, optional): Maximum number of results to return (mostly for testing quickly). For unrestricted, use -1. Defaults to -1.

#     Returns:
#         int: Number of XML files downloaded.
#     """
#     if df is None:
#         df = get_xml_urls(df=None, use_cache=use_cache, cache=cache, max_res=max_res)
#     res = _cache_xml_chillfully(df, max_res, use_cache)
#     return res


# def _cache_xml_chillfully(df, max_res, use_cache):  # TODO: needs some work
#     res = 0
#     for _, row in df.iterrows():
#         if max_res > 0 and res >= max_res:
#             return res
#         url = row['xml_url']
#         filename = url.rsplit('/', 1)[1]
#         path = _xml_data_prefix + filename
#         did_cache = _cache_xml(path, url, use_cache)
#         if did_cache:
#             res += 1
#             if verbose:
#                 print(f'Cached {res}', end=_backspace_print)
#     return res


# def _cache_xml(path, url, use_cache) -> bool:  # TODO: needs some work
#     """Cache XML from URL. Return True, if it got cached, False if already existed."""
#     if use_cache and os.path.exists(path):
#         return False
#     with open(path, 'w', encoding='utf-8') as f:
#         data = _load_xml_content(url)
#         f.write(data)
#         if not data:
#             print(f'No data loaded for {url}')
#         return True


def _load_xml_content(url):  # TODO: somewhere here, ensure that the content is actually XML
    """Load XML content from URL, ensuring the encoding is correct."""
    log.debug(f"Loading XML Content: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        return None
    bytes_ = response.text.encode(response.encoding)
    if bytes_[0] == 255:
        try:
            bytes_ = bytes_[2:]
            txt = bytes_.decode('utf-16le')
            txt = txt.replace('UTF-16', 'UTF-8', 1)
            log.warning(f"Found XML in encoding 'UTF-16-LE'. Converted to 'UTF-8'. Check if data was lost in the process.\nXML: {url}")
            return txt
        except Exception as e:  # TODO: never seems to be the case
            log.warning(f"Failed to convert 'UTF-16-LE'. Fall back to 'ISO-8859-1'. Check if data was lost in the process.\nXML: {url}")
            log.exception(e)
            return response.text.replace('iso-8859-1', 'UTF-8')
    else:
        try:
            txt = bytes_.decode('utf-8')  # 0xe1
            log.debug("Encodung: UTF-8")
            return txt
        except Exception as e:  # TODO: the case a lot, find a better way?
            log.warning(f"Failed to convert 'UTF-8'. Fall back to 'ISO-8859-1'. Check if data was lost in the process.\nXML: {url}")
            log.exception(e)
            return response.text.replace('iso-8859-1', 'UTF-8')


# Look up shelfmarks
# ------------------

# def get_shelfmarks(df: pd.DataFrame = None, use_cache: bool = True, cache: bool = True, max_res: int = -1) -> pd.DataFrame:
#     """Look up all manuscript shelfmarks.

#     The dataframe contains the following collumns:
#     - Manuscript ID (`id`)
#     - Shelfmark (`shelfmark`)

#     Args:
#         df (pd.DataFrame, optional): Dataframe containing the available XML URLs. If `None` is passed, `get_xml_urls()` will be called. Defaults to None.
#         use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
#         cache (bool, optional): Flag true if result should be written to cache. Defaults to True.
#         max_res (int, optional): Maximum number of results to return (mostly for testing quickly). For unrestricted, use -1. Defaults to -1.

#     Returns:
#         pd.DataFrame: Dataframe containing shelfmarks.
#     """
#     # TODO: this should happen when XML is loaded first... do we even need the separate shelfmark thingie?
#     if use_cache and os.path.exists(_shelfmark_path):
#         res = pd.read_csv(_shelfmark_path)
#         if res is not None and not res.empty and _is_shelfmarks_complete(res):  # LATER: should work from that, in case of partially finished loading
#             log.debug('Loaded shelfmarks from cache.')
#             return res
#     if df is None:
#         df = get_xml_urls(df=None, use_cache=use_cache, cache=cache, max_res=max_res)
#     if max_res > 0 and max_res < len(df.index):
#         df = df[:max_res]

#     iter_ = _get_shelfmarks(df)
#     res = pd.DataFrame(iter_, columns=['id', 'shelfmark']).sort_values(by='id').drop_duplicates()
#     if cache:
#         res.to_csv(_shelfmark_path, encoding='utf-8', index=False)
#     return res


# def _is_shelfmarks_complete(df: pd.DataFrame) -> bool:
#     ids_a = len(get_ids()['id'].unique())
#     ids_b = len(df['id'].unique())
#     return ids_a == ids_b


# def _get_shelfmarks(df: pd.DataFrame):
#     for _, row in df.iterrows():
#         shelfmark = _get_shelfmark(row['xml_file'])
#         yield row['id'], shelfmark


def _get_shelfmark(soup) -> str:
    msid = soup.find('msIdentifier')
    if msid:
        idno = msid.idno
        if idno:
            sm = idno.getText()
            log.debug(f"Extracted shelfmark: {sm}")
            return sm


# Access XML Directly
# -------------------

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
    df = get_xml_urls()
    hits = df.loc[df['id'] == id_]
    for _, row in hits.iterrows():
        lang = row['lang']
        url = row['xml_url']
        res[lang] = load_xml(url, use_cache=use_cache, cache=cache)
    return res


def crawl(use_cache: bool = False, prog: Optional[guiUtils.Progress] = None):
    """crawl everything as fast as possible"""
    log.debug(f'Start: {datetime.now()}')
    log.info('Start crawling')

    if prog:
        prog.set_steps(3)
    if not use_cache:
        log.debug('Removing cache...')
        _wipe_cache()
        log.debug('Done removing cache.')

    log.debug('Loading collections...')
    colls = get_collections()
    log.debug(f"Done loading collections. Found {len(colls.index)} collections containing {colls['ms_count'].sum()} manuscripts.")
    if prog:
        prog.increment('Loaded Collections')
        prog_ids = prog.next_subtask("Loading MS IDs", "Done loading MS IDs")

    log.debug("Loading Manuscript IDs...")
    ids = get_ids(df=colls, prog=prog_ids)
    log.debug(f"Done loading Manuscript IDs. Found {len(ids.index)} unique identifiers.")

    if prog:
        prog_urls = prog.next_subtask("Loading Manuscript XMLs", "Done loading XMLs")
    log.debug("Loading XML URLs...")
    urls = get_xml_urls(df=ids, prog=prog_urls)
    log.debug(f"Done loading URLs. Found {len(urls.index)} URLs.")

    # log.debug("Downloading XML Data...")
    # loaded_xmls = cache_all_xml_data(df=urls)
    # log.debugt(f"Done loading XMLs. Downloaded {loaded_xmls} files.")
    # if prog:
    #     prog.increment()

    # log.debug("Extracting shelfmarks...")
    # shelfmarks = get_shelfmarks(df=urls)
    # log.debug(f"Done extracting shelfmarks. Found {len(shelfmarks.index)} shelfmarks.")
    # log.info('done Crawling.')
    # if prog:
    #     prog.increment()

    log.debug(f'Finished: {datetime.now()}')


def _wipe_cache():  # TODO: expand, should involve handler stuff too
    xmls = glob.glob(_xml_data_prefix + '*.xml')
    for xml in xmls:
        os.remove(xml)
    if os.path.exists(_coll_path):
        os.remove(_coll_path)
    if os.path.exists(_id_path):
        os.remove(_id_path)
    if os.path.exists(_xml_url_path):
        os.remove(_xml_url_path)
    if os.path.exists(_shelfmark_path):
        os.remove(_shelfmark_path)
