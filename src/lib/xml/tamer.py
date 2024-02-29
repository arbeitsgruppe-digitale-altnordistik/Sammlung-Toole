from pathlib import Path
from typing import Iterable, Iterator, Optional, Tuple
from lxml import etree
import statistics
import lib.utils as utils
import lib.xml.metadata as metadata
from lib.constants import PERSON_DATA_PATH
from lib.manuscripts import CatalogueEntry
from lib.people import Person


log = utils.get_logger(__name__)
nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}


# Region: Persons extracted and delivered

def get_ppl_names() -> list[Person]:
    # This works and gives the correct number of people /SK
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
        all_first_names = name_tag.findall('forename', nsmap)
        all_last_names = name_tag.findall('surname', nsmap)
        first_name_clean = [name.text for name in all_first_names if name.text]
        if first_name_clean:
            first_name = " ".join(first_name_clean)
        else:
            first_name = ""
        last_name = " ".join([name.text for name in all_last_names])
        if not first_name and not last_name and name_tag.text:
            last_name = name_tag.text
        current_pers = Person(id_, first_name, last_name)
        res.append(current_pers)
    return res


# End Region

# Region: Catalog data delivery

def _get_all_data_from_files(files: Iterable[Path]) -> Iterator[CatalogueEntry]:
    for f in files:
        ele = _load_xml_contents(f)
        filename = f.name
        if ele is not None:
            yield _parse_xml_content(ele, filename)


def get_metadata_from_files(files: Iterable[Path]) -> list[CatalogueEntry]:
    data = _get_all_data_from_files(files)
    return list(data)


# End Region
# Region: XML parser


def _parse_xml_content(root: etree._Element, filename: str) -> CatalogueEntry:
    log.info(f"Parsing metadata: {filename}")
    shelfmark = _get_shelfmark(root, filename)
    full_id = _find_full_id(root)
    ms_nickname = _get_shorttitle(root, full_id, shelfmark)
    country, settlement, repository = _get_ms_location(root, full_id)
    origin = get_origin(root, full_id)
    date, tp, ta, meandate, yearrange = get_date(root, full_id)  # DONE!
    support = get_support(root)
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


# End Region
# Region: XML utils


def _load_xml_contents(path: Path) -> Optional[etree._Element]:
    try:
        log.info(f"Loading XML file: {path}")
        tree: etree._ElementTree = etree.parse(path, None)
        root: etree._Element = tree.getroot()
        return root
    except etree.XMLSyntaxError:
        if path.is_relative_to('data/handrit'):  # it's a real file not a test file
            log.warning(f"{path}: Broken XML!")
        return None
    except OSError:
        if path.is_relative_to('data/handrit'):  # it's a real file not a test file
            log.warning(f"{path}: Non existent XML!")
        return None


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


def _get_shorttitle(root: etree._Element, ms_id: str, shelfmark: str) -> str:
    # It appears that all warnings are correct, i.e. that those MSs really don't have a shorttitle /SK
    # 14-Feb caught some more weird exceptions that f*d up rebuild
    # TODO: Remove comment
    head = root.find(".//head", root.nsmap)
    summary = root.find(".//summary", root.nsmap)
    try:
        if head:
            title_raw = head.findall("title", root.nsmap)
        else:
            title_raw = summary.findall("title", root.nsmap)
        if title_raw:
            if len(title_raw) > 1:
                ttls = []
                for i in title_raw:
                    ttl = i.text
                    ttls.append(ttl)
                title = " ".join(ttls)
            else:
                title = title_raw[0].text
            if title:
                res = title.replace('\n', ' ')
                res = res.replace('\t', ' ')
                res = ' '.join(res.split())
                return str(res)
            else:
                return shelfmark
        else:
            return shelfmark
    except Exception:
        log.warning(f"{ms_id} title extraction failed.")
        return shelfmark


def _get_shelfmark(root: etree._Element, log_id: str) -> str:
    # This works without fail on Feb 2, 2023; no errors in logs /SK
    # TODO: Remove comment
    try:
        idno = root.find('.//msDesc/msIdentifier/idno', root.nsmap)
        if idno is not None:
            return str(idno.text)
        else:
            return ""
    except Exception:
        log.warning(f"Faild to load Shelfmark XML: {log_id}")
        return ""


def _find_id(root: etree._Element) -> str:
    id_ = _find_full_id(root)
    if 'da' in id_ or 'en' in id_ or 'is' in id_:
        id1 = id_.rsplit('-', 1)
        id_ = id1[0]
    return str(id_)


def _find_full_id(root: etree._Element) -> str:
    # This also produces no errors on Feb 3 23 /SK
    # TODO: Remove comment
    id_raw = root.find('.//msDesc', root.nsmap)
    try:
        id_ = id_raw.attrib['{http://www.w3.org/XML/1998/namespace}id']
        return str(id_)
    except Exception:
        log.warning(f"Failed to find the ID of {id_raw}")


def _get_ms_location(root: etree._Element, xml_id: str) -> tuple[str | None, str | None, str | None]:
    # Feb 03 23: This seems to be working fine as of today; exceptions caught do not have the req'd infos in their XMLs /SK
    # TODO: Remove above
    # TODO: Implement none handling. I am very sorry for doing "None", I know its a massive smell /SK
    ms_id = root.find(".teiHeader/fileDesc/sourceDesc/msDesc/msIdentifier", nsmap)

    if ms_id:
        co = ms_id.find("country", nsmap)
        try:
            country = co.text
        except Exception:
            country = "None"
            log.debug(f"No country info found for {xml_id}")
        se = ms_id.find("settlement", nsmap)
        try:
            settlement = se.text
        except Exception:
            settlement = "None"
            log.debug(f"No settlement info found for {xml_id}")
        re = ms_id.find("repository", nsmap)
        try:
            repository = re.text
        except Exception:
            repository = "None"
            log.debug(f"No repo info found for {xml_id}")
        return country, settlement, repository
    else:
        log.debug(f"No location info found for {xml_id}")
        return "None", "None", "None"


def get_origin(root: etree._Element, msid: str) -> str:
    """Get manuscript's place of origin.

    Args:
        root:

    Returns:
        str: country name
    """
    # Feb-23: Working
    # TODO: remove note
    # TODO: Implement handling of none /SK
    unkown_msg = "origin unkown"
    orig_place = root.find(".//origPlace", root.nsmap)
    if orig_place:
        pretty_place = _get_key(orig_place)
        if pretty_place:
            return pretty_place
        if orig_place.text:
            return str(orig_place.text)
    else:
        log.info(f"No place of origin found for{msid}")
        return unkown_msg
    if orig_place is None:
        return unkown_msg


def get_date(root: etree._Element, msid: str) -> Tuple[str, int, int, int, int]:
    # TODO: Redesign /SK
    # TODO: Implement None
    tags = root.findall(".//origDate", root.nsmap)
    if len(tags) > 1:
        for t in tags:
            try:
                res = _find_dates(t, msid)
                if res:
                    return res
            except Exception:
                print("This didn`t work")
    else:
        tag = root.find(".//origDate", root.nsmap)
        return _find_dates(tag=tag, msid=msid)


def get_support(root: etree._Element) -> str:
    # TODO: Implement handling of None, I feel horrible about "None" /SK
    """Get supporting material (paper or parchment).

    Args:
        root (etree._Element): etree XML element object

    Returns:
        str: supporting material
    """
    support_desc = root.find('.//supportDesc', root.nsmap)
    if support_desc is not None:
        support = support_desc.attrib['material']
        if support == "chart":
            pretty_support = "Paper"
        elif support == "perg":
            pretty_support = "Parchment"
        else:
            try:
                pretty_support = support.text
            except Exception:
                pretty_support = "None"
    else:
        pretty_support = "None"
    if not pretty_support:
        pretty_support = "None"
    return pretty_support


# End Region
# Region: Helper Functions


def _get_key(leek: etree._Element) -> str | None:
    """Find key identifying the country and return country name.

    Args:
        leek (etree._element): xml-tag

    Returns:
        str: country name
    """
    # TODO: Replace key_dict with dict from authority file
    # TODO: Implement handling of None
    if leek.get('key') is not None:
        country_key = str(leek.attrib['key'])
        country_key = country_key.lower().replace(".", "")
        key_dict: dict[str, str] = {
            "is": "Iceland",
            "dk": "Denmark",
            "fo": "Faroe Islands",
            "no": "Norway",
            "se": "Sweden",
            "ka": "Canada",
            "copen01": "Copenhagen",
            "reykj01": "Reykjavík",
            "fr": "France",
            "usa": "USA"
        }
        return key_dict.get(country_key, country_key)
    else:
        return "None"


def tp_ta_tag_cleaner(tag: etree._Element.tag) -> Tuple[str, int, int, int, int]:
    not_before = str(tag.attrib["notBefore"])
    not_after = str(tag.attrib["notAfter"])

    """Sometimes dates will be given with greater specificity than just a year
    i.e. '1699-01-01'. Then we want to get just the year.
    We just need to make sure we catch dates like '01-01-1699' as well."""
    if len(not_before) >= 5:
        not_before = date_normalizer(not_before)

    if len(not_after) >= 5:
        not_after = date_normalizer(not_after)

    date = f"{not_before}-{not_after}"
    tp = int(not_before)
    ta = int(not_after)
    meandate = int(statistics.mean([int(tp), int(ta)]))
    yearrange = int(ta) - int(tp)

    return date, tp, ta, meandate, yearrange


def date_normalizer(input_date: str) -> str:
    date_parts = None
    if "." in input_date:
        date_parts = input_date.split(".")
    if "-" in input_date:
        date_parts = input_date.split("-")
    res = "0"
    if date_parts is not None:
        for x in date_parts:
            if len(x) == 4:
                res = x
                break
    return res


def when_tag_cleaner(tag: etree._Element.tag) -> Tuple[str, int, int, int, int]:
    date = str(tag.attrib["when"])
    normalized_date = date
    if len(normalized_date) > 4:
        normalized_date = date_normalizer(normalized_date)
    tp = int(normalized_date)
    ta = int(normalized_date)
    meandate = tp
    yearrange = 0
    return date, tp, ta, meandate, yearrange


def from_to_tag_cleaner(tag: etree._Element.tag) -> Tuple[str, int, int, int, int]:
    _tp = str(tag.attrib["from"])
    _ta = str(tag.attrib["to"])

    if len(_tp) > 4:
        _tp = date_normalizer(_tp)
    if len(_ta) > 4:
        _ta = date_normalizer(_ta)

    date = f"{_tp}-{_ta}"
    tp = int(_tp)
    ta = int(_ta)
    meandate = int(statistics.mean([int(tp), int(ta)]))
    yearrange = int(ta) - int(tp)

    return date, tp, ta, meandate, yearrange


def _find_dates(tag: etree._Element.tag, msid: str) -> Tuple[str, int, int, int, int]:
    backstop_res = "None", 0, 0, 0, 0
    if tag is None:
        return backstop_res
    try:
        if tag.get("notBefore") is not None and tag.get("notAfter") is not None:
            res = tp_ta_tag_cleaner(tag=tag)
        elif tag.get("when") is not None:
            res = when_tag_cleaner(tag=tag)
        elif tag.get("from") and tag.get("to") is not None:
            res = from_to_tag_cleaner(tag=tag)
        else:
            date_raw = tag.text
            if date_raw is not None:
                if len(date_raw) > 4:
                    date = date_normalizer(date_raw)
                else:
                    date = date_raw
                res = date, int(date), int(date), int(date), 0
            else:
                res = backstop_res
        return res
    except Exception:
        log.debug(f"{msid}: Date extraction failed.")
        return backstop_res
