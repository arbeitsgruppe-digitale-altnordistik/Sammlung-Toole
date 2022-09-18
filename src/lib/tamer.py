import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List,  Tuple

import pandas as pd
import requests
import src.lib.metadata as metadata
import src.lib.utils as utils
from bs4 import BeautifulSoup
from lxml import etree
from src.lib.constants import *

log = utils.get_logger(__name__)
nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}

# Data preparation
# ----------------
"""These functions are used to load relevant data into the data handler. All data the handler uses on 
initialization should come from here."""
# TODO: Update doc string


def __load_xml_contents() -> pd.DataFrame:
    # Deprecated
    outDF = pd.DataFrame(columns=['shelfmark', 'content'])
    for individual_xml_file in Path(XML_BASE_PATH).rglob('*.xml'):
        log.debug(f'Loading: {str(individual_xml_file)}')
        file_contents = _load_xml_file(str(individual_xml_file))
        shelfmark = _get_shelfmark(file_contents)
        outDF = outDF.append({'shelfmark': shelfmark, 'content': file_contents}, ignore_index=True)
    return outDF


def load_xml_contents(path: Path) -> etree._Element:
    tree: etree._ElementTree = etree.parse(path)
    root = tree.getroot()
    return root


def parse_xml_content(root: etree._Element) -> tuple[tuple[Any], set[str], set[str]]:  # TODO: Metatype for first tuple (Metadata)
    shelfmark = _get_shelfmark(root)
    ppl_raw = root.findall(".//name", nsmap)
    ppl: list[str] = []
    for pers in ppl_raw:
        persID = pers.attrib['key']
        ppl.append(persID)
    txts_raw = root.find(".//msItem")
    txts: list[str] = []
    for txt in txts_raw:
        lvl = txt.attrib["n"]
        if not "." in lvl:
            title_raw = txt.find("title", nsmap)  # TODO: Streamline, should be able to select tags directly /SK
            title = title_raw.text
            txts.append(title)
    ms_nickname = _get_shorttitle(root)
    country, settlement, repository = metadata.get_msID(root)
    origin = metadata.get_origin(root)
    date, tp, ta, meandate, yearrange = metadata.get_date(root)
    support = metadata.get_support(root)
    # TODO: Continue here. Think about "get_extent" func
    # -> streamline and adapt to get number of folios right /SK
    pass


def _get_shorttitle(root: etree._Element) -> str:
    head = root.find(".//head", nsmap)
    summary = root.find(".//summary", nsmap)
    if head:
        title = head.title
    else:
        title = summary.title
    try:
        res = title.replace('\n', ' ')
        res = res.replace('\t', ' ')
        res = ' '.join(res.split())
        return str(res)
    except:
        return str(title)


def make_work(files: Iterable[Path]) -> Iterator[tuple[tuple[Any], set[str], set[str]]]:
    for f in files:
        ele = load_xml_contents(f)
        data = parse_xml_content(ele)
        yield data


def unpack_work(files: Iterable[Path]) -> tuple[list[tuple[Any]], list[set[str]], list[set[str]]]:
    x = make_work(files)
    meta_data: list[tuple[Any]] = []
    ppl: list[set[str]] = []
    txts: list[set[str]] = []
    for y in x:
        m, p, t = y
        meta_data.append(m)
        ppl.append(p)
        txts.append(t)
    return meta_data, ppl, txts


def _load_xml_file(xml_file: str) -> str:
    with open(xml_file, encoding='utf-8', mode='r+') as file:
        try:
            return file.read()
        except Exception as e:
            log.exception(f'Failed to read file {xml_file}')
            return ""


def _get_shelfmark(root: etree._Element) -> str:
    try:
        idno = root.find('.//msDesc/msIdentifier/idno', root.nsmap)
        if idno is not None:
            return str(idno.text)
        else:
            return ""
    except Exception:
        log.exception(f"Faild to load Shelfmark XML:\n\n{root}\n\n")
        return ""


# def deliver_handler_data() -> pd.DataFrame:
#     # Deprecated
#     """Will check if data is available and return a dataframe to the data handler.
#     DataFrame has the following columns:
#     'shelfmark': Shelfmark of the individual MS
#     'content': The XML of the file as string
#     """
#     outDF = load_xml_contents()
#     return outDF
# TODO: Clean up


def get_ppl_names() -> List[Tuple[str, str, str]]:
    """Delivers the names found in the handrit names authority file.
    Returns list of tuples containing: first name, last name, and alphanumeric, unique ID in that order.
    """
    res: List[Tuple[str, str, str]] = []
    tree = etree.parse(PERSON_DATA_PATH)
    root = tree.getroot()
    ppl = root.findall(".//person", nsmap)
    print(len(ppl))
    for pers in ppl:
        id_ = pers.get('{http://www.w3.org/XML/1998/namespace}id')
        name_tag = pers.find('persName', nsmap)
        firstNameS = name_tag.findall('forename', nsmap)
        lastNameS = name_tag.findall('surname', nsmap)
        firstNameClean = [name.text for name in firstNameS if name.text]
        if firstNameClean:
            firstName = " ".join(firstNameClean)
        else:
            firstName = ""
        lastName = " ".join([name.text for name in lastNameS])
        if not firstName and not lastName:
            if name_tag.text:
                lastName = name_tag.text
        currPers = (firstName, lastName, id_)
        res.append(currPers)
    return res


def get_person_mss_matrix(df: pd.DataFrame) -> List[Tuple[int, str, str]]:
    # CHORE: document
    res: List[Tuple[int, str, str]] = []
    idgen = 1
    for _, row in df.iterrows():
        ms_id = row['full_id']
        soup = row['soup']
        persons = soup.find_all('name', {'type': 'person'})
        # LATER: note that <handNote scribe="XYZ"/> won't be found like this (see e.g. Steph01-a-da.xml)
        if persons:
            for person in persons:
                pers_id = person.get('key')
                if pers_id:
                    curT = (idgen, pers_id, ms_id)
                    res.append(curT)
                    idgen += 1  # TODO: idgen not needed, adjust SQL to use autincrement on primary key column
    return res


def get_text_mss_matrix(df: pd.DataFrame) -> List[Tuple[str, str]]:
    # CHORE: document
    res: List[Tuple[str, str]] = []
    for _, row in df.iterrows():
        ms_id = row['full_id']
        soup = row['soup']
        texts = [t[1] for t in metadata._title_from_soup(soup) if t[1]]
        if texts:
            for text in texts:
                if text:
                    curT = (ms_id, text)
                    res.append(curT)
    return res


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
    try:
        id = id_raw.get('xml:id')
    except:
        id = 'ID-ERR-01'
        log.error("Some soups are to salty. An unkown error occured and an unknown MS could not be souped or strained.")
    return str(id)


def get_ms_info(soup: BeautifulSoup) -> pd.Series:
    '''Will deliver a data frame to be crunched into SQL
    '''
    # DEPRECATED! This should pretty much all be broken now /SK
    # TODO: Cleanup/purge /SK

    shorttitle = metadata.get_shorttitle(soup)
    _, country, settlement, repository = metadata.get_msID(soup)
    origin = metadata.get_origin(soup)
    date, tp, ta, meandate, yearrange = metadata.get_date(soup)
    support = metadata.get_support(soup)
    folio = metadata.get_folio(soup)
    height, width = metadata.get_dimensions(soup)
    creator = metadata.get_creators(soup)
    extent = metadata.get_extent(soup)
    description = metadata.get_description(soup)
    id = _find_id(soup)
    full_id = _find_full_id(soup)
    filename = _find_filename(soup)
    log.debug(f"Sucessfully souped and strained {full_id}")

    return pd.Series({"shorttitle": shorttitle,
                      "country": country,
                      "settlement": settlement,
                      "repository": repository,
                      "origin": origin,
                      "date": date,
                      "terminusPostQuem": tp,
                      "terminusAnteQuem": ta,
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
    res: List[str] = []
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, 'lxml')
    resDiv = soup.find(class_="t-data-grid-pager")
    if not resDiv:
        return list(url)
    getNumberRow = resDiv.get_text()
    muchResultsWow = False
    if "..." in getNumberRow:
        muchResultsWow = True
    if muchResultsWow:
        pageNosRaw = resDiv.find_all("a")
        totalNo = 1
        for i in pageNosRaw:
            ix = i.get('title')
            ino = [int(x) for x in ix.split() if x.isdigit()]
            print(ino)
            for no in ino:
                if no > totalNo:
                    totalNo = no
        for i in range(totalNo):
            htm = requests.get(res[i]).text
            soup = BeautifulSoup(htm, 'lxml')
            links = soup.select("div.t-data-grid-pager > a")
            urls = [l['href'] for l in links]
            for u in urls:
                if u not in res:
                    res.append(u)
        if len(res) != totalNo:
            print("Something smells fucky.")
        return res
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
    shelfmarks = [ss.get_text() for ss in subsoups]
    shelfmarks = [sm.strip() for sm in shelfmarks]
    log.info(f"At 'get_shelfmarks', I still have {len(shelfmarks)}")
    log.debug(f"These are: {shelfmarks}")
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


def get_person_names_inverse(person_names: Dict[str, str]) -> Dict[str, List[str]]:
    res: Dict[str, List[str]] = {}
    for k, v in person_names.items():
        res[v] = res.get(v, []) + [k]
    return res
