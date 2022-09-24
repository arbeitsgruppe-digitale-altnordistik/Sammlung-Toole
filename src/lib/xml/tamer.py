from pathlib import Path
from typing import Iterable, Iterator, Optional

import src.lib.xml.metadata as metadata
import src.lib.utils as utils
from lxml import etree
from src.lib.constants import PERSON_DATA_PATH

MetadataRowType = tuple[
    str, str, str, str, str, str, str, int, int, int, int, str, int, str, str, str, str, str, str, str, str
]

log = utils.get_logger(__name__)
nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}

# Data preparation
# ----------------
"""These functions are used to load relevant data into the SQLite database."""


def _load_xml_contents(path: Path) -> Optional[etree._Element]:
    try:
        tree: etree._ElementTree = etree.parse(path, None)
        root: etree._Element = tree.getroot()
        return root
    except Exception:
        log.exception(f"{path}: Broken XML!")
        return None


def _parse_xml_content(root: etree._Element) -> tuple[MetadataRowType, list[tuple[str, str]], list[tuple[str, str]]]:
    shelfmark = _get_shelfmark(root)
    full_id = _find_full_id(root)
    ms_nickname = _get_shorttitle(root, full_id)
    country, settlement, repository = metadata.get_ms_id(root)
    origin = metadata.get_origin(root)
    date, tp, ta, meandate, yearrange = metadata.get_date(root)
    support = metadata.get_support(root)
    folio = metadata.get_folio(root)
    height, width, extent, description = metadata.get_description(root)
    handrit_id = _find_id(root)
    filename = _find_filename(root)
    creator = metadata.get_creators(root)
    txts = _get_txt_list_from_ms(root, full_id)
    ppl = _get_ppl_from_ms(root, full_id)
    ms_x_ppl = [(x, full_id) for x in list(dict.fromkeys(ppl))]  # TODO-BL: Understand this?!
    ms_x_txts = [(full_id, x) for x in list(dict.fromkeys(txts))]  # TODO-BL: Understand this?!
    if len(ppl) == 0:
        log.info(f"{full_id} doesn't have any people living in it. Check!")
        ppl.append("N/A")  # TODO-BL: Understand this?!
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


def _get_ppl_from_ms(root: etree._Element, full_id: str) -> list[str]:
    ppl_raw = root.findall(".//name", nsmap)
    if ppl_raw is None:
        log.warn(f"{full_id} doesn't have any people living in it. Check!")
        return ["N/A"]  # TODO-BL: Understand this?!
    ppl = [person.get('key') for person in ppl_raw if person.get('key') is not None]
    return ppl


# TODO-BL: split this up to reduce complexity
def _get_txt_list_from_ms(root: etree._Element, ms_id: str) -> list[str]:
    txts_raw = root.findall(".//msItem", nsmap)
    if txts_raw is None:
        log.warn(f"{ms_id} apparently has no texts. Check if this is correct!")
        return ["N/A"]  # TODO-BL: Understand this?!
    txts: list[str] = []
    for txt in txts_raw:
        lvl = txt.get('n')
        if lvl is None:
            title_raw = txt.find("title", nsmap)
            if title_raw is not None:
                title: str = title_raw.text
                if title is not None:
                    if "\n" in title:
                        title = "".join(title.splitlines())
                    title = " ".join(title.split())  # There are excessive spaces in the XML. This gets rid of them /SK
                    txts.append(title)
    if len(txts) == 0:
        txts.append("N/A")
    return txts


def _get_shorttitle(root: etree._Element, ms_id: str) -> str:
    head = root.find(".//head", root.nsmap)
    summary = root.find(".//summary", root.nsmap)
    if head is None and summary is None:
        log.warn(f"{ms_id} has no nickname or it is stored in a weird way")
        return "N/A"
    if head is not None:
        title_raw = head.find("title", root.nsmap)
    else:
        title_raw = summary.find("title", root.nsmap)
    title = title_raw.text
    try:
        res = title.replace('\n', ' ')
        res = res.replace('\t', ' ')
        res = ' '.join(res.split())
        return str(res)
    except Exception:  # TODO-BL: tidy this part up
        log.exception("Weird stuff going on in getting 'shorttitle'")
        return str(title)


def _get_all_data_from_files(files: Iterable[Path]) -> Iterator[tuple[MetadataRowType, list[tuple[str, str]], list[tuple[str, str]]]]:
    for f in files:
        ele = _load_xml_contents(f)
        if ele:
            yield _parse_xml_content(ele)


def get_metadata_from_files(files: Iterable[Path]) -> tuple[list[MetadataRowType], list[list[tuple[str, str]]], list[list[tuple[str, str]]]]:
    data = _get_all_data_from_files(files)
    # LATER: the rest can be simplified to `return tuple(zip(*data))` but typing is not happy
    meta_data: list[MetadataRowType] = []
    ppl: list[list[tuple[str, str]]] = []
    txts: list[list[tuple[str, str]]] = []
    for m, p, t in data:
        meta_data.append(m)
        ppl.append(p)
        txts.append(t)
    return meta_data, ppl, txts


def _get_shelfmark(root: etree._Element) -> str:
    try:
        idno = root.find('.//msDesc/msIdentifier/idno', root.nsmap)
        if idno is not None:
            return str(idno.text)
        else:
            return ""
    except Exception:
        # TODO: do we ever get here? may be caught by the if-else and return empty
        log.exception(f"Faild to load Shelfmark XML:\n\n{root}\n\n")
        return ""


def get_ppl_names() -> list[tuple[str, str, str]]:
    """Delivers the names found in the handrit names authority file.
    Returns list of tuples containing: first name, last name, and alphanumeric, unique ID in that order.
    """
    res: list[tuple[str, str, str]] = []
    tree = etree.parse(PERSON_DATA_PATH, None)
    root = tree.getroot()
    ppl = root.findall(".//person", nsmap)
    print(len(ppl))  # TODO: remove or log
    for pers in ppl:
        id_ = pers.get('{http://www.w3.org/XML/1998/namespace}id')
        name_tag = pers.find('persName', nsmap)
        firstNameS = name_tag.findall('forename', nsmap)  # TODO: tidy up
        lastNameS = name_tag.findall('surname', nsmap)
        firstNameClean = [name.text for name in firstNameS if name.text]
        if firstNameClean:
            firstName = " ".join(firstNameClean)
        else:
            firstName = ""
        lastName = " ".join([name.text for name in lastNameS])
        if not firstName and not lastName and name_tag.text:
            lastName = name_tag.text
        currPers = (id_, firstName, lastName)
        res.append(currPers)
    return res


# Data extraction
# ---------------


def _find_filename(root: etree._Element) -> str:
    id_ = _find_full_id(root)
    return f"{id_}.xml"


def _find_id(root: etree._Element) -> str:
    id_ = _find_full_id(root)
    if 'da' in id_ or 'en' in id_ or 'is' in id_:
        id1 = id_.rsplit('-', 1)
        id_ = id1[0]
    return str(id_)


def _find_full_id(root: etree._Element) -> str:
    id_raw = root.find('.//msDesc', root.nsmap)
    try:
        id_ = id_raw.attrib['{http://www.w3.org/XML/1998/namespace}id']
    except Exception:
        id_ = 'ID-ERR-01'
        log.exception("Some soups are to salty. An unkown error occured.")
    return str(id_)
