from pathlib import Path
from typing import Iterable, Iterator, Optional

from lxml import etree

import src.lib.utils as utils
import src.lib.xml.metadata as metadata
from src.lib.constants import PERSON_DATA_PATH
from src.lib.manuscripts import CatalogueEntry
from src.lib.people import Person

log = utils.get_logger(__name__)
nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}


def _load_xml_contents(path: Path) -> Optional[etree._Element]:
    try:
        log.info(f"Loading XML file: {path}")
        tree: etree._ElementTree = etree.parse(path, None)
        root: etree._Element = tree.getroot()
        return root
    except etree.XMLSyntaxError:
        if path.is_relative_to('data/handrit'):  # it's a real file not a test file
            log.exception(f"{path}: Broken XML!")
        return None
    except OSError:
        if path.is_relative_to('data/handrit'):  # it's a real file not a test file
            log.exception(f"{path}: Non existent XML!")
        return None


def _parse_xml_content(root: etree._Element, filename: str) -> CatalogueEntry:
    log.info(f"Parsing metadata: {filename}")
    shelfmark = _get_shelfmark(root)
    full_id = _find_full_id(root)
    ms_nickname = _get_shorttitle(root, full_id)
    country, settlement, repository = metadata.get_ms_origin(root)
    origin = metadata.get_origin(root)
    date, tp, ta, meandate, yearrange = metadata.get_date(root)
    support = metadata.get_support(root)
    folio = metadata.get_folio(root)
    height, width, extent, description = metadata.get_description(root)
    handrit_id = _find_id(root)
    creator = metadata.get_creators(root)
    txts = _get_txt_list_from_ms(root)
    if txts == []:
        log.warn(f"{full_id} apparently has no texts. Check if this is correct!")
    ppl = _get_ppl_from_ms(root)
    if ppl == []:
        log.warn(f"{full_id} doesn't have any people living in it. Check!")
    log.debug(f"Sucessfully processed {shelfmark}/{full_id}")
    return CatalogueEntry(
        catalogue_id=full_id,
        shelfmark=shelfmark,
        manuscript_id=handrit_id,
        catalogue_filename=filename,
        title=ms_nickname,
        description=description,
        date_string=date,
        terminus_post_quem=tp,
        terminus_ante_quem=ta,
        date_mean=meandate,
        dating_range=yearrange,
        support=support,
        folio=folio,
        height=height,
        width=width,
        extent=extent,
        origin=origin,
        creator=creator,
        country=country,
        settlement=settlement,
        repository=repository,
        texts=txts,
        people=ppl
    )


def _get_ppl_from_ms(root: etree._Element) -> list[str]:
    """gets a list of person IDs, given an XML document"""
    ppl_raw = root.findall(".//name", nsmap)
    if ppl_raw is None:
        return []
    ppl: list[str] = [person.get('key') for person in ppl_raw if person.get('key') is not None]
    log.debug(f"Loaded people from xml: {len(ppl)}")
    return list(set(ppl))


def _get_txt_list_from_ms(root: etree._Element) -> list[str]:
    txts_raw = root.findall(".//msItem", nsmap)
    if txts_raw is None:
        return []
    txts: list[str] = []
    for txt in txts_raw:
        # lvl = txt.get('n')
        # print(lvl)
        # if lvl is None:
        title_raw = txt.find("title", nsmap)
        # rubric_raw = txt.find("rubric", nsmap)
        # print(lvl)
        # print(title_raw.text if title_raw is not None else "null")
        # print(rubric_raw)
        if title_raw is not None:
            title: str = title_raw.text
            if title is not None:
                if "\n" in title:
                    title = "".join(title.splitlines())
                title = " ".join(title.split())  # There are excessive spaces in the XML. This gets rid of them /SK
                txts.append(title)
        # elif rubric_raw is not None:
        #     rubric: str = rubric_raw.text
        #     if rubric is not None:
        #         if "\n" in rubric:
        #             rubric = "".join(rubric.splitlines())
        #         rubric = " ".join(rubric.split())
        #         txts.append(rubric)
    return list(set(txts))


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
    if title_raw is None:
        log.debug(f"No title present in manuscript: {ms_id}")
        return "N/A"
    title = title_raw.text
    if not title:
        return "N/A"
    try:
        res = title.replace('\n', ' ')
        res = res.replace('\t', ' ')
        res = ' '.join(res.split())
        return str(res)
    except Exception:
        log.exception("Weird stuff going on in getting 'shorttitle'")
        return str(title)


def _get_all_data_from_files(files: Iterable[Path]) -> Iterator[CatalogueEntry]:
    for f in files:
        ele = _load_xml_contents(f)
        filename = f.name
        if ele is not None:
            yield _parse_xml_content(ele, filename)


def get_metadata_from_files(files: Iterable[Path]) -> list[CatalogueEntry]:
    data = _get_all_data_from_files(files)
    return list(data)


def _get_shelfmark(root: etree._Element) -> str:
    try:
        idno = root.find('.//msDesc/msIdentifier/idno', root.nsmap)
        if idno is not None:
            return str(idno.text)
        else:
            return ""
    except Exception:
        log.exception(f"Faild to load Shelfmark XML: {root}")
        return ""


def get_ppl_names() -> list[Person]:
    """Delivers the names found in the handrit names authority file.
    Returns list of Person value objects.
    """
    res: list[Person] = []
    tree = etree.parse(PERSON_DATA_PATH, None)
    root = tree.getroot()
    ppl = root.findall(".//person", nsmap)
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
        if not firstName and not lastName and name_tag.text:
            lastName = name_tag.text
        currPers = Person(id_, firstName, lastName)
        res.append(currPers)
    return res


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
