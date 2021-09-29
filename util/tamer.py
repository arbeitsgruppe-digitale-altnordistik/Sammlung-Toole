import os
from typing import List, Set
from bs4 import BeautifulSoup
import pandas as pd
from bs4 import BeautifulSoup
import util.metadata as metadata
import util.utils as utils
from util.constants import *
import zipfile
import glob
from lxml import etree
import requests
import time
from pathlib import Path


log = utils.get_logger(__name__)

# Data preparation
# ----------------


def has_data_available() -> bool:
    """Check if data is available"""
    xmls = glob.glob(PREFIX_XML_DATA + '*.xml')
    if xmls:
        log.info('XMLs found in directory.')
        return True
    log.info('No downloaded XMLs found.')
    return False


def unzipper() -> bool:
    """Unzips xml files from source directory into target directory. 
    Returns True on success.
    """
    zip = glob.glob(PREFIX_XML_RAW + 'xml.zip')
    if zip:
        with zipfile.ZipFile(zip[0], 'r') as file:
            file.extractall(PREFIX_XML_DATA)
            xmls = glob.glob(PREFIX_XML_DATA+'xml/*.xml')
            for xml in xmls:
                p = Path(xml)
                dest = os.path.join(PREFIX_XML_DATA, p.name)
                os.replace(xml, dest)
            os.rmdir(PREFIX_XML_DATA+'xml')
            log.info('Extracted XMLs from zip file.')
            return True
    log.info('No zip file found. No data. Nothing to do.')
    return False


def _get_files_in_place() -> bool:
    """Will make sure there are XMLs in data folder, unzip if not, logs error if 
    there are no XMLs and no zip file. Returns false if no XMLs and no zip, true on
    success.
    """
    has_data = has_data_available()
    if not has_data:
        unzip = unzipper()
        if not unzip:
            log.error('Could not find any data!')
            return False
    return True


def load_xml_contents() -> pd.DataFrame:
    all_stored_xmls = glob.iglob(PREFIX_XML_DATA + '*xml')
    outDF = pd.DataFrame(columns=['shelfmark', 'content'])
    for individual_xml_file in all_stored_xmls:
        file_contents = _load_xml_file(individual_xml_file)
        shelfmark = _get_shelfmark(file_contents)
        outDF = outDF.append({'shelfmark': shelfmark, 'content': file_contents}, ignore_index=True)
    return outDF


def _load_xml_file(xml_file: str) -> str:
    with open(xml_file, encoding='utf-8', mode='r+') as file:
        return file.read()


def _get_shelfmark(content: str) -> str:
    try:
        root = etree.fromstring(content.encode())
        idno = root.find('.//msDesc/msIdentifier/idno', root.nsmap)
        # log.debug(f'Shelfmark: {etree.tostring(idno)}')
        log.debug(f'Shelfmark: {idno.text}')
        if idno is not None:
            return str(idno.text)
        else:
            return ""
    except Exception:
        log.exception(f"Faild to load Shelfmark XML:\n\n{content}\n\n")
        return ""


def deliver_handler_data() -> pd.DataFrame:
    """Will check if data is available and return a dataframe to the data handler.
    DataFrame has the following columns:
    'shelfmark': Shelfmark of the individual MS
    'content': The XML of the file as string
    """
    data_check = _get_files_in_place()
    if not data_check:
        return
    outDF = load_xml_contents()
    return outDF

# Helpers
# -------


def _get_mstexts(soup: BeautifulSoup) -> List[str]:
    msItems = soup.find_all('msContents')
    curr_titles = []
    for i in msItems:
        title = i.title.get_text()
        curr_titles.append(title)

    return curr_titles

# Data extraction
# ---------------


def _find_filename(soup: BeautifulSoup) -> str:
    id = _find_full_id(soup)
    return f"{id}.xml"


def _find_id(soup: BeautifulSoup) -> str:
    id = _find_full_id(soup)
    if 'da' in id or 'en' in id or 'is' in id:
        id1 = id.rsplit('-', 1)
        id = id1[0]
    return str(id)


def _find_full_id(soup: BeautifulSoup) -> str:
    id_raw = soup.find('msDesc')
    id = id_raw.get('xml:id')
    return str(id)


def get_msinfo(soup: BeautifulSoup) -> pd.Series:
    shorttitle = metadata.get_shorttitle(soup)
    signature, country, settlement, repository = metadata.get_msID(soup)
    origin = metadata.get_origin(soup)
    date, tp, ta, meandate, yearrange = metadata.get_date(soup)
    support = metadata.get_support(soup)
    folio = metadata.get_folio(soup)
    height, width = metadata.get_dimensions(soup)
    creator = metadata.get_creator(soup)
    extent = metadata.get_extent(soup)
    description = metadata.get_description(soup)
    id = _find_id(soup)
    full_id = _find_full_id(soup)
    filename = _find_filename(soup)

    return pd.Series({"shorttitle": shorttitle,
                      "country": country,
                      "settlement": settlement,
                      "repository": repository,
                      "origin": origin,
                      "date": date,
                      "Terminus post quem": tp,
                      "Terminus ante quem": ta,
                      "meandate": meandate,
                      "yearrange": yearrange,
                      "support": support,
                      "folio": folio,
                      "height": height,
                      "width": width,
                      "extent": extent,
                      "description": description,
                      "creator": creator,
                      "id": id,
                      "full_id": full_id,
                      "filename": filename})


def efnisordResult(inURL: str) -> List[str]:
    resultPage = requests.get(inURL).content
    pho = BeautifulSoup(resultPage, 'lxml')
    theGoods = pho.find('tbody')
    identifierSoup = theGoods.find_all(class_='id')
    identifierList = []
    for indi in identifierSoup:
        identifier = indi.get_text()
        identifierList.append(identifier)
    return identifierList


def get_search_result_pages(url: str) -> List[str]:
    """Get multiple result pages from search with 26+ hits.
    This function returns a list of all result pages from one search result,
    if the search got too many hits to display on one page.
    Args:
        url (str): URL to a multi page search result
    Returns:
        List[str]: a list with URLs for all pages of the search result
    """
    res = [url]
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, 'lxml')
    links = soup.select("div.t-data-grid-pager > a")
    urls = [l['href'] for l in links]
    for u in urls:
        if u not in res:
            res.append(u)
    return res


def get_shelfmarks(url: str) -> List[str]:
    """Get Shelfmarks from an URL
    This function returns a list of strings containing shelfmarks from a page on handrit.is.
    Args:
        url (str): a URL to a search result page on handrit.is
    Returns:
        List[str]: A list of Shelfmarks
    """
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, 'lxml')
    subsoups = soup.select("td.shelfmark")
    print(subsoups)
    shelfmarks = [ss.get_text() for ss in subsoups]
    shelfmarks = [sm.strip() for sm in shelfmarks]
    print(f"At 'get_shelfmarks', I still have {len(shelfmarks)}, and these are:")
    print(shelfmarks)
    return shelfmarks


def get_shelfmarks_from_urls(urls: List[str]) -> List[str]:
    results = []
    if len(urls) == 1:
        url = urls[0]
        results += get_shelfmarks(url)
        return list(set(results))
    for url in urls:
        results += get_shelfmarks(url)
        time.sleep(0.2)
    return list(set(results))


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
