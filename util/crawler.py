from typing import Any, Generator, List, Optional, Tuple
import os
import glob
from datetime import datetime
import pickle
from threading import Thread

import pandas as pd
import requests
# from bs4 import BeautifulSoup
from stqdm import stqdm
from tqdm import tqdm
from lxml import etree

# from streamlit.report_thread import add_report_ctx

from util import utils
from util.utils import Settings
from util.constants import HANDLER_BACKUP_PATH_MSS, CRAWLER_PATH_IDS
from util.constants import PREFIX_XML_DATA, PREFIX_XML_URL, PREFIX_BACKUPS
from util.constants import CRAWLER_PATH_CONTENT_PICKLE, CRAWLER_PICKLE_PATH, CRAWLER_PATH_URLS, CRAWLER_PATH_COLLECTIONS, CRAWLER_PATH_POTENTIAL_XMLS, CRAWLER_PATH_404S


log = utils.get_logger(__name__)
settings = Settings.get_settings()


# Crawl Collections
# -----------------

def crawl_collections() -> pd.DataFrame:
    """Load all collections from handrit.is.

    The dataframe contains the following informations:
    - Collection ID (`collection`)
    - Number of Manuscripts listed for the Collection (`ms_count`)
    - Collection URL (`url`)

    Returns:
        pd.DataFrame: Data frame containing basic information on collections.
    """
    log.info('Loading Collections')
    if settings.use_cache and os.path.exists(CRAWLER_PATH_COLLECTIONS):
        cols = pd.read_csv(CRAWLER_PATH_COLLECTIONS)
        if cols is not None and not cols.empty:
            log.info('Loaded collections from cache.')
            return cols
    cols = _load_collections()
    if settings.cache:
        cols.to_csv(CRAWLER_PATH_COLLECTIONS, encoding='utf-8', index=False)
    log.info(f"Loaded {len(cols.index)} collections.")
    return cols


def _load_collections() -> pd.DataFrame:
    """Load collections from website"""
    soup = utils.get_soup('https://handrit.is/#collection', 'lxml')
    collection_tags = soup.find_all('div', attrs={'class': 'collection'})
    collections = [(c.find('span', attrs={'class': 'mark'}).text,
                    int(c.find('span', attrs={'class': 'count'}).text.split()[0]),
                    c.find('a', attrs={'class': 'viewall'})['href'].rsplit(';')[0] + '?showall.browser=1',) for c in collection_tags]
    df = pd.DataFrame(collections, columns=['collection', 'ms_count', 'url'])
    return df


# Crawl Manuscript IDs
# --------------------

def crawl_ids(df: pd.DataFrame = None, prog: Any = None) -> pd.DataFrame:
    """Load all manuscript IDs.

    The dataframe contains the following collumns:
    - Collection ID (`collection`)
    - Manuscript ID (`id`)

    Args:
        df (pd.DataFrame, optional): Dataframe containing the available collections on handrit. If `None` is passed, `get_collections()` will be called. Defaults to None.
        prog (Any, optional): Optional streamlit progress bar. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe containing the manuscript IDs.
    """
    # LATER: could add progressbar to this one too
    log.info('Loading Manuscript IDs')
    if df is None:
        df = crawl_collections()
    preloaded_ids = None
    if settings.use_cache and os.path.exists(CRAWLER_PATH_IDS):
        ids = pd.read_csv(CRAWLER_PATH_IDS)
        if ids is not None and not ids.empty:
            if _is_ids_complete(ids):
                log.info('Loaded manuscript IDs from cache.')
                return ids
            else:
                preloaded_ids = _load_ids(df, ids)
                # LATER: improve working with half finished caches (e.g. after max_res)
                pass
    if preloaded_ids is not None and not preloaded_ids.empty:
        ids = preloaded_ids  # LATER: this doesn't do anything
    else:
        ids = _load_ids(df)
    if len(ids.index) >= settings.max_res:
        ids = ids[:settings.max_res]
    ids.sort_values(by=['collection', 'id'])
    if settings.cache:
        ids.to_csv(CRAWLER_PATH_IDS, encoding='utf-8', index=False)
    log.info(f"Loaded {len(ids.index)} IDs.")
    return ids


def _is_ids_complete(ids: pd.DataFrame) -> bool:
    """indicates if the number of IDs matches the number indicated by the collections"""
    colls = crawl_collections()
    if colls['ms_count'].sum() == len(ids.index):
        return True
    if len(ids.index) >= settings.max_res:
        log.info('Not all IDs available, but enough to meet max_res limitation')
        return True
    log.warning('Number of manuscripts does not match the number indicated by collections')
    return False


def _load_ids(df: pd.DataFrame, preloaded: Optional[pd.DataFrame] = None, prog: Any = None) -> pd.DataFrame:
    """Load IDs"""
    # LATER: progress bar
    def get_iter(df: pd.DataFrame) -> Generator[Tuple[str, str], None, None]:
        hits = 0
        if preloaded is not None and not preloaded.empty:
            for _, row in preloaded.iterrows():
                if hits >= settings.max_res:
                    break
                col = row['collection']
                id_ = row['id']
                log.debug(f'Skipped ID: {id_}')
                yield (col, id_,)
                hits += 1
        cols = list(df.collection)
        for col in cols:
            if hits >= settings.max_res:
                break
            url = df.loc[df.collection == col, 'url'].values[0]
            for res in _download_ids_from_url(url, col):
                if hits >= settings.max_res:
                    break
                if preloaded is not None and res[1] in preloaded['id']:
                    log.debug(f'Skipped {res} because it had been preloaded')
                    continue
                yield res
                hits += 1

    iterator_ = get_iter(df)
    res = pd.DataFrame(iterator_, columns=['collection', 'id'])
    return res


def _download_ids_from_url(url: str, col: str) -> List[Tuple[str, str]]:
    """get IDs from a collection URL"""
    soup = utils.get_soup(url, 'lxml')
    res = []
    for td in soup.find_all('td', attrs={'class': 'id'}):
        res.append((col, td.text))
    return res


# Crawl XML URLs
# --------------

def crawl_xmls(df: pd.DataFrame = None, prog: Any = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load all manuscript XMLs.
    # CHORE: update

    The dataframe contains the following collumns:
    - Collection ID (`collection`)
    - Manuscript ID (`id`)
    - language (`lang`)
    - URL to XML (`xml_url`)

    Args:
        df (pd.DataFrame, optional): Dataframe containing the available manuscript IDs. If `None` is passed, `get_ids()` will be called. Defaults to None.
        prog (streamlit.beta_container, optional): streamlit context for displaying progress bars. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe containing manuscript URLs.
    """
    if settings.use_cache and os.path.exists(CRAWLER_PATH_URLS):
        log.info("Loading XML URLs from cache.")
        import pdb; pdb.set_trace()
        xmls = pd.read_csv(CRAWLER_PATH_URLS)
        if os.path.exists(CRAWLER_PATH_CONTENT_PICKLE):
            with open(CRAWLER_PATH_CONTENT_PICKLE, mode='rb') as file:
                contents = pickle.load(file)
        else:
            contents = _load_all_contents(xmls, prog=prog)
        if _is_urls_complete(xmls):
            return xmls, contents
        else:
            pass
            # LATER: do something here
    log.info("Loading XML URLs.")
    if df is None:
        df = crawl_ids(df=None)
    if settings.max_res < len(df.index):
        df = df[:settings.max_res]
    log.info("Building potential URLs")
    log.debug(f'IDs for building URLs: {len(df.index)}')
    # TODO: handle loading cached XMLs first of all, as it should be the quickest way
    potentials = _get_potential_xmls(df, prog=prog)
    log.debug(f"Potential URLs: {len(potentials.index)}")
    log.info("Loading actual XMLs")
    xmls = _get_existing_xmls(potentials, prog=prog)
    xmls['shelfmark'] = xmls['content'].apply(_get_shelfmark)
    contents = xmls[['content', 'xml_file']]
    contents['path'] = PREFIX_XML_DATA + contents['xml_file']
    xmls.drop(columns=['content'], inplace=True)
    xmls.drop(columns=['exists'], inplace=True)
    if settings.cache:
        xmls.to_csv(CRAWLER_PATH_URLS, encoding='utf-8', index=False)
        with open(CRAWLER_PATH_CONTENT_PICKLE, mode='wb') as file:
            pickle.dump(contents, file=file)
    return xmls, contents


def _get_existing_xmls(potentials: pd.DataFrame, prog: Any = None) -> pd.DataFrame:
    if len(potentials.index) > (3 * settings.max_res):
        potentials = potentials[:(settings.max_res * 3)]
    log.debug(f'Loading XMLs from {len(potentials.index)} potential URLs')
    if prog:
        with prog:
            stqdm.pandas(desc="Loading XMLs")
            potentials['content'] = potentials.progress_apply(_get_xml_content, axis=1)
            # TODO: why is this not working anymore? or is it?
    else:
        tqdm.pandas(desc="Loading XMLs")
        potentials['content'] = potentials.progress_apply(_get_xml_content, axis=1)
    potentials['exists'] = potentials['content'].apply(lambda x: True if x else False)
    df = potentials[potentials['exists'] == True]
    if len(df.index) > settings.max_res:
        df = df[:settings.max_res]
    return df


def _get_xml_content(row: pd.Series) -> Optional[str]:
    path = f"{PREFIX_XML_DATA}{row['xml_file']}"
    if settings.use_cache and os.path.exists(path):
        with open(path, encoding='utf-8', mode='r+') as file:
            return file.read()
    content = _load_xml_content(row['xml_url'])
    if not content:
        return None
    if settings.cache:
        th = Thread(target=_cache_xml_content, args=(content, path))
        th.start()
    return content


def _load_all_contents(xmls: pd.DataFrame = None, prog: Any = None) -> pd.DataFrame:
    if prog:
        with prog:
            res = pd.DataFrame()
            res['xml_url'] = xmls['xml_url']
            stqdm.pandas(desc="Loading XMLs")
            res['content'] = res.progress_apply(_get_xml_content, axis=1)
    else:
        res = pd.DataFrame()
        res['xml_url'] = xmls['xml_url']
        tqdm.pandas(desc="Loading XMLs")
        res['content'] = res.apply(_get_xml_content, axis=1)
    return res


def _get_potential_xmls(id_df: pd.DataFrame, prog: Any = None) -> pd.DataFrame:
    non_existing: List[str] = []
    if settings.use_cache and os.path.exists(CRAWLER_PATH_404S):
        with open(CRAWLER_PATH_404S, 'r+', encoding='utf-8') as f:
            non_existing = f.read().split('\n')
        log.info(f'Number of known non-existing URLs: {len(non_existing)}')
    if settings.use_cache and os.path.exists(CRAWLER_PATH_POTENTIAL_XMLS):
        log.info("Loading XML URLs from cache.")
        res = pd.read_csv(CRAWLER_PATH_POTENTIAL_XMLS)
        colls = crawl_collections()
        if len(res.index) >= colls['ms_count'].sum() * 3:
            not_nons = res[~res['xml_url'].isin(non_existing)].reindex()
            log.debug(f'Reduced options of potential URLs from {len(res.index)} to {len(not_nons.index)} by disregarding known non-existing URLs')
            return not_nons

    def iterate() -> pd.DataFrame:
        df = pd.DataFrame(columns=['collection', 'id', 'lang', 'xml_url'])
        log.info(f'Number of known non-existing URLs: {len(non_existing)}')
        for _, row in stqdm(id_df.iterrows(), total=len(id_df.index), desc="Calculating Potential URLs"):
            langs = ('en', 'da', 'is')
            for l in langs:
                file = f'{row[1]}-{l}.xml'
                url = f'{PREFIX_XML_URL}{file}'
                if url in non_existing:
                    non_existing.remove(url)
                    log.debug(f'Skipping URL because previousely it did not exist: {url}')
                    continue
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
    if settings.cache:
        df.to_csv(CRAWLER_PATH_POTENTIAL_XMLS, encoding='utf-8', index=False)
    return df


def _is_urls_complete(df: pd.DataFrame) -> bool:
    ids_a = len(crawl_ids()['id'].unique())
    ids_b = len(df['id'].unique())
    if ids_a == ids_b:
        return True
    if len(df.index) >= settings.max_res:
        log.debug('Not all URLs available but enough to satisfy max_res')
        return True
    log.info('Cache not containing enough URLs')
    return False


def _cache_xml_content(content: str, path: str) -> None:
    with open(path, mode='w+', encoding='utf-8') as file:
        file.write(content)


def _load_xml_content(url: str) -> Optional[str]: # NOTE: Keep this function for later!
    """Load XML content from URL, ensuring the encoding is correct."""
    response = requests.get(url)
    if response.status_code != 200:
        if settings.cache:
            with open(CRAWLER_PATH_404S, mode='a', encoding='utf-8') as f:
                f.write(url + '\n')
        return None
    bytes_ = response.text.encode(response.encoding)
    if bytes_.startswith(b'<!DOCTYPE html'):
        log.error(f"Content is not XML: {url}")
        return None
    if bytes_[0] == 255:
        try:
            bytes_ = bytes_[2:]
            txt = bytes_.decode('utf-16le')
            txt = txt.replace('UTF-16', 'UTF-8', 1)
            log.warning(f"Found XML in encoding 'UTF-16-LE'. Converted to 'UTF-8'. Check if data was lost in the process. XML: {url}")
            return txt
        except Exception as e:
            log.warning(f"Failed to convert 'UTF-16-LE'. Fall back to 'ISO-8859-1'. Check if data was lost in the process. XML: {url}")
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
                return None
        elif bytes_.startswith(b'<?xml version="1.0" encoding="iso-8859-1"'):
            try:
                txt = bytes_.decode('iso-8859-1')
                txt = txt.replace('iso-8859-1', 'UTF-8')
                log.info(f"Loaded {url} - Encoding: ISO-8859-1 changed to UTF-8")
                return txt
            except Exception as e:
                log.warning(f"Failed to convert {url}")
                log.exception(e)
                return None
        else:
            log.warning(f"Unknown encoding: {url} Assuming UTF-8")
            try:
                txt = bytes_.decode('utf-8')
                return '<?xml version="1.0" encoding="UTF-8"?>\n' + txt
            except Exception as e:
                log.warning(f"Failed to convert {url}")
                log.exception(e)
                return None


def _get_shelfmark(content: str) -> str:
    try:
        root = etree.fromstring(content.encode())
        idno = root.find('.//msDesc/msIdentifier/idno', root.nsmap)
        log.debug(f'Shelfmark: {etree.tostring(idno)}')
        if idno is not None:
            return str(idno.text)
        else:
            return ""
    except Exception:
        log.exception(f"Faild to load Shelfmark XML:\n\n{content}\n\n")
        return ""


# Access XML Directly
# -------------------

# LATER: remove unless really needed

# def load_xml(url: str) -> BeautifulSoup:
#     """Load XML from URL.

#     Args:
#         url (str): URL of an XML on handrit.is.

#     Returns:
#         BeautifulSoup: XML content.
#     """
#     filename = url.rsplit('/', 1)[1]
#     return load_xml_by_filename(filename=filename)


# def load_xml_by_filename(filename: str) -> BeautifulSoup:
#     """Load XML by file name.

#     Args:
#         filename (str): XML file name.

#     Returns:
#         BeautifulSoup: XML content.
#     """
#     path = PREFIX_XML_DATA + filename
#     if settings.use_cache and os.path.exists(path):
#         with open(path, 'r', encoding='utf-8') as f:
#             return BeautifulSoup(f, 'xml')
#     xml = _load_xml_content(PREFIX_XML_URL + filename)
#     if settings.cache and xml:
#         with open(path, 'w', encoding='utf-8') as f:
#             f.write(xml)
#     soup = BeautifulSoup(xml, 'xml')
#     return soup


# def load_xmls_by_id(id_: str) -> Dict[str, str]:
#     """Load XML(s) by manuscript ID.

#     Args:
#         id_ (str): Manuscript ID

#     Returns:
#         dict: dict of the XMLs found by this ID. Follows the structure `{language: BeautifulSoup}` (i.e. `{'da': 'soup', 'is': 'soup}`).
#     """
#     res = {}
#     df, _ = crawl_xmls()
#     hits = df.loc[df['id'] == id_]
#     for _, row in hits.iterrows():
#         lang = row['lang']
#         url = row['xml_url']
#         res[lang] = load_xml(url)
#     return res


def has_data_available() -> bool:
    """Check if data is available"""
    xmls = glob.glob(PREFIX_XML_DATA + '*.xml')
    if xmls and \
            os.path.exists(CRAWLER_PATH_COLLECTIONS) and \
            os.path.exists(CRAWLER_PATH_IDS) and \
            os.path.exists(CRAWLER_PATH_URLS) and \
            os.path.exists(CRAWLER_PATH_POTENTIAL_XMLS) and \
            os.path.exists(CRAWLER_PATH_CONTENT_PICKLE):
        return True
    return False


def crawl(use_cache: bool = False, prog: Any = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """crawl everything as fast as possible"""
    log.info(f'Start crawling: {datetime.now()}')
    _ensure_directories()

    settings.use_cache = use_cache

    if not settings.use_cache:
        _wipe_cache()
        log.info('Done removing cache.')

    colls = crawl_collections()
    log.info(f"Done loading collections. Found {len(colls.index)} collections containing {colls['ms_count'].sum()} manuscripts.")
    ids = crawl_ids(df=colls)
    log.info(f"Done loading Manuscript IDs. Found {len(ids.index)} unique identifiers.")

    urls, contents = crawl_xmls(df=ids, prog=prog)
    log.info(f"Done loading URLs. Found {len(urls.index)} URLs.")

    log.info(f'Finished: {datetime.now()}')

    return urls, contents


def _wipe_cache() -> None:
    """Remove all cached files"""
    xmls = glob.glob(PREFIX_XML_DATA + '*.xml')
    for xml in xmls:
        os.remove(xml)
    if os.path.exists(CRAWLER_PATH_COLLECTIONS):
        os.remove(CRAWLER_PATH_COLLECTIONS)
    if os.path.exists(CRAWLER_PATH_IDS):
        os.remove(CRAWLER_PATH_IDS)
    if os.path.exists(CRAWLER_PATH_URLS):
        os.remove(CRAWLER_PATH_URLS)
    if os.path.exists(CRAWLER_PATH_POTENTIAL_XMLS):
        os.remove(CRAWLER_PATH_POTENTIAL_XMLS)
    if os.path.exists(CRAWLER_PATH_CONTENT_PICKLE):
        os.remove(CRAWLER_PATH_CONTENT_PICKLE)
    if os.path.exists(HANDLER_BACKUP_PATH_MSS):
        os.remove(HANDLER_BACKUP_PATH_MSS)
    if os.path.exists(CRAWLER_PICKLE_PATH):
        os.remove(CRAWLER_PICKLE_PATH)
    if os.path.exists(CRAWLER_PATH_404S):
        os.remove(CRAWLER_PATH_404S)


def _ensure_directories() -> None:
    """Ensure all caching directories exist"""
    os.makedirs(PREFIX_XML_DATA, exist_ok=True)
    os.makedirs(PREFIX_BACKUPS, exist_ok=True)


if __name__ == '__main__':
    crawl_xmls()