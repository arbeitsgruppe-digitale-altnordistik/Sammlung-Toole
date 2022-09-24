import time
from pathlib import Path
from turtle import width
from typing import Any, Dict, Iterable, Iterator, List,  Tuple

import pandas as pd
import requests
import src.lib.xml.metadata as metadata
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
    try:
        tree: etree._ElementTree = etree.parse(path)
        root = tree.getroot()
        return root
    except:
        log.error(f"{path}: Broken XML!")


def parse_xml_content(root: etree._Element) -> tuple[tuple[Any], list[tuple[str, str]], list[tuple[str, str]]]:  # TODO: Metatype for first tuple (Metadata)
    shelfmark = _get_shelfmark(root)
    full_id = _find_full_id(root)
    ms_nickname = _get_shorttitle(root, full_id)
    country, settlement, repository = metadata.get_msID(root)
    origin = metadata.get_origin(root)
    date, tp, ta, meandate, yearrange = metadata.get_date(root)
    support = metadata.get_support(root)
    folio = metadata.get_folio(root)
    height, width, extent, description = metadata.get_description(root)
    handrit_id = _find_id(root)
    filename = _find_filename(root)
    creator = metadata.get_creators(root)
    txts = get_txt_list_from_ms(root, full_id)
    ppl_raw = root.findall(".//name", nsmap)
    ppl: list[str] = []
    if ppl_raw is not None:  # TODO: Refactor
        for pers in ppl_raw:
            try:
                pers_id = pers.attrib['key']
                ppl.append(pers_id)
            except:
                pass
    else:
        log.info(f"{full_id} doesn't have any people living in it. Check!")
        ppl.append("N/A")
    ms_x_ppl = [(x, full_id) for x in list(dict.fromkeys(ppl))]
    ms_x_txts = [(full_id, x) for x in list(dict.fromkeys(txts))]
    if len(ppl) == 0:
        log.info(f"{full_id} doesn't have any people living in it. Check!")
        ppl.append("N/A")
    log.debug(f"Sucessfully processed {shelfmark}/{full_id}")
    res = (
        (
            shelfmark,
            ms_nickname,
            country,
            settlement,
            repository,
            origin,
            date,
            tp,
            ta,
            meandate,
            yearrange,
            support,
            folio,
            height,
            width,
            extent,
            description,
            creator,
            handrit_id,
            full_id,
            filename
        ),
        ms_x_ppl,
        ms_x_txts,
    )
    if creator != 'NULL':
        print(creator)
    return res


def get_txt_list_from_ms(root: etree.Element, ms_id: str) -> list[str]:
    txts_raw = root.findall(".//msItem", nsmap)
    txts: list[str] = []
    if txts_raw is not None:
        for txt in txts_raw:
            good = True
            try:
                lvl = txt.attrib["n"]
                if "." in lvl:
                    good = False
            except:
                pass
            if good:
                title_raw = txt.find("title", nsmap)  # TODO: Streamline, should be able to select tags directly /SK
                if title_raw is not None:
                    title: str = title_raw.text
                    if title is not None:
                        if "\n" in title:
                            title = "".join(title.splitlines())
                        title = " ".join(title.split())  # There are excessive spaces in the XML. This gets rid of them /SK
                        txts.append(title)
    else:
        log.info(f"{ms_id} apparently has no texts. Check if this is correct!")
        txts.append("N/A")
    if len(txts) == 0:
        txts.append("N/A")
    return txts


def _get_shorttitle(root: etree._Element, ms_id: str) -> str:
    head = root.find(".//head", root.nsmap)
    summary = root.find(".//summary", root.nsmap)
    try:
        if head is not None:
            title_raw = head.find("title", root.nsmap)
            if title_raw is None and summary is not None:
                title_raw = summary.find("title", root.nsmap)
        elif summary is not None:
            title_raw = summary.find("title", root.nsmap)
        title = title_raw.text
    except:
        log.info(f"{ms_id} has no nickname or it is stored in a weird way")
        # TODO: Catch weird XML better.
        title = "N/A"
    try:
        res = title.replace('\n', ' ')
        res = res.replace('\t', ' ')
        res = ' '.join(res.split())
        return str(res)
    except:
        return str(title)


def make_work(files: Iterable[Path]) -> Iterator[tuple[tuple[Any], list[tuple[str, str]], list[tuple[str, str]]]]:
    for f in files:
        ele = load_xml_contents(f)
        if ele:
            data = parse_xml_content(ele)
            yield data
        else:
            pass


def unpack_work(files: Iterable[Path]) -> tuple[list[tuple[Any]], list[list[tuple[str, str]]], list[list[tuple[str, str]]]]:
    x = make_work(files)
    meta_data: list[tuple[Any]] = []
    ppl: list[list[tuple[str, str]]] = []
    txts: list[list[tuple[str, str]]] = []
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
        currPers = (id_, firstName, lastName)
        res.append(currPers)
    return res


# Data extraction
# ---------------


def _find_filename(root: etree._Element) -> str:
    id = _find_full_id(root)
    return f"{id}.xml"


def _find_id(root: etree._Element) -> str:
    id = _find_full_id(root)
    if 'da' in id or 'en' in id or 'is' in id:
        id1 = id.rsplit('-', 1)
        id = id1[0]
    return str(id)


def _find_full_id(root: etree._Element) -> str:
    id_raw = root.find('.//msDesc', root.nsmap)
    try:
        id = id_raw.attrib['{http://www.w3.org/XML/1998/namespace}id']
    except:
        id = 'ID-ERR-01'
        log.error("Some soups are to salty. An unkown error occured.")
    return str(id)


def get_ms_info(soup: BeautifulSoup) -> pd.Series:
    '''Will deliver a data frame to be crunched into SQL
    '''
    # DEPRECATED! This should pretty much all be broken now /SK
    # TODO: Cleanup/purge /SK

    # shorttitle = metadata.get_shorttitle(soup)
    # _, country, settlement, repository = metadata.get_msID(soup)
    # origin = metadata.get_origin(soup)
    # date, tp, ta, meandate, yearrange = metadata.get_date(soup)
    # support = metadata.get_support(soup)
    # folio = metadata.get_folio(soup)
    # height, width = metadata.get_dimensions(soup)
    # creator = metadata.get_creators(soup)
    # extent = metadata.get_extent(soup)
    # description = metadata.get_description(soup)
    # id = _find_id(soup)
    # full_id = _find_full_id(soup)
    # filename = _find_filename(soup)
    # log.debug(f"Sucessfully souped and strained {full_id}")

    # return pd.Series({"shorttitle": shorttitle,
    #                   "country": country,
    #                   "settlement": settlement,
    #                   "repository": repository,
    #                   "origin": origin,
    #                   "date": date,
    #                   "terminusPostQuem": tp,
    #                   "terminusAnteQuem": ta,
    #                   "meandate": meandate,
    #                   "yearrange": yearrange,
    #                   "support": support,
    #                   "folio": folio,
    #                   "height": height,
    #                   "width": width,
    #                   "extent": extent,
    #                   "description": description,
    #                   "creator": creator,
    #                   "id": id,
    #                   "full_id": full_id,
    #                   "filename": filename})


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
