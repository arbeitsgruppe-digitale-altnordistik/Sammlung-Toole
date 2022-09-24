import copy
import re
import statistics
import urllib
from typing import List, Optional, Tuple

import src.lib.xml.tamer as tamer
from bs4 import BeautifulSoup
from bs4.element import Tag
from lxml import etree
from src.lib import utils

log = utils.get_logger(__name__)
nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}


# Utlity Functions
# ----------------


# def get_soup(url: str) -> BeautifulSoup:  # TODO: should become obsolete
#     """Get a BeautifulSoup object from a URL

#     Args:
#         url (str): url

#     Returns:
#         bs4.BeautifulSoup: BeautifulSoup object representation of the HTML/XML page.
#     """

#     sauce = urllib.request.urlopen(url).read()
#     soup = BeautifulSoup(sauce, "xml")

#     return soup


def get_cleaned_text(carrot: Tag) -> str:
    """Get human-readable text from xml-tag.

    Args:
        carrot (bs4.element.Tag): xml-tag

    Returns:
        str: human-readable text
    """
    if not carrot:
        return ""
    res: Optional[str] = carrot.get_text()
    if not res:
        return ""
    res = res.replace('\n', ' ')
    res = res.replace('\t', ' ')
    res = ' '.join(res.split())

    return res


def _get_digits(text: str) -> int:
    """Gets digits from text.

    Args:
        text (str): text

    Returns:
        int: digits from text
    """
    s = ""
    for x in text:
        if x.isdigit():
            s += x
    if s:
        i = int(s)
    else:
        i = 0

    return i


# Pull meta data
# ---------------


def _get_key(leek: etree._Element) -> Optional[str]:
    """Find key identifying the country and return country name.

    Args:
        leek (bs4.element.Tag): xml-tag

    Returns:
        str: country name
    """

    if leek.attrib['key']:
        key = leek.attrib['key']
        key = key.lower()
        if key == "is":
            pretty_key = "Iceland"
        elif key == "dk":
            pretty_key = "Denmark"
        elif key == "fo":
            pretty_key = "Faroe Islands"
        elif key == "no":
            pretty_key = "Norway"
        elif key == "se":
            pretty_key = "Sweden"
        elif key == "ka":
            pretty_key = "Canada"
        else:
            pretty_key = "!! unknown country key"
            log.warning(f"unknown country key: {key}. (Fix function get_key)")
    else:
        return None

    return pretty_key


def get_origin(root: etree._Element) -> str:
    """Get manuscript's place of origin.

    Args:
        root: 

    Returns:
        str: country name
    """
    origPlace = root.find(".//origPlace", root.nsmap)
    # TODO-BL: tidy up
    try:
        try:
            pretty_origPlace = _get_key(origPlace)
            if not pretty_origPlace:
                return "Origin unknown"
        except Exception:
            log.exception("Issue with getting origin")
            pretty_origPlace = str(origPlace.text)
    except Exception:
        log.exception("Issue with getting origin")
        pretty_origPlace = "Origin unknown"
    return pretty_origPlace


def get_creators(root: etree._Element) -> str:  # TODO: Implement a method that works with foreign keys
    """Get creator(s). Function for new SQLite backend.

    Args:
        root (etree._Element): etree XML element object
    Returns:
        str: creator name(s)
    """
    # TODO-BL: make strict division between SQLite and XML
    hands = root.findall(".//handDesc", root.nsmap)
    pplIDs: List[str] = []

    if hands is not None:
        try:
            for hand in hands:
                potentials = hand.findall(".//name", root.nsmap)
                for p in potentials:
                    scribe = p.text
                    if scribe is not None:
                        scribe = "".join(scribe.splitlines())
                        scribe = " ".join(scribe.split())
                        if scribe is not None:  # Somehow, a few None values made their way into the final db...? /SK
                            pplIDs.append(scribe)
        except Exception:
            # LATER: find out why, if that happens
            fKey = "NULL"
            pplIDs.append(fKey)
    else:
        # LATER: find out why, if that happens
        fKey = "NULL"
        pplIDs.append(fKey)
    if len(pplIDs) == 0:
        fKey = "NULL"
        pplIDs.append(fKey)
    res = "; ".join(set(pplIDs))
    return res


def get_support(root: etree._Element) -> str:
    """Get supporting material (paper or parchment).

    Args:
        root (etree._Element): etree XML element object

    Returns:
        str: supporting material
    """
    supportDesc = root.find('.//supportDesc', root.nsmap)
    if supportDesc:
        support = supportDesc.attrib['material']
        if support == "chart":
            pretty_support = "Paper"
        elif support == "perg":
            pretty_support = "Parchment"
        else:
            pretty_support = support
            try:
                pretty_support = support.text
            except Exception:
                pretty_support = ""
    else:
        pretty_support = ""

    return pretty_support


def get_folio(root: etree._Element) -> int:
    # TODO: Update docstring! /SK
    """Returns: total of folios.

    Find <extent> and make copy.
    Get rid of info that might bias number of folio:
        Find <dimensions> and destroy all its tags from <extent>.
        Find <locus> and destroy all its tags from <extent>.
    Get cleaned text from remaining info in <extent>: folio number.
    Try:
        - If total of folio given, take it.
        - If not, get rid of pages refering to empty pages
        and of further unnecessary info about the manuscript in brackets, possibly containing digits.
        - Check whether calculated number of folio is reasonable (< 1.000) - add [?] if not -
        or whether it is 0 - write n/a.
    Except:
        - return: n/a

    Args:
        root (etree._Element): etree XML element object

    Returns:
        int: total of folios
    """
    # TODO: look into this method... can this be streamlined?
    extent = root.find('.//extent', root.nsmap)

    if not extent:
        return 0
    extent_copy = copy.copy(extent)

    dimensions = extent_copy.findall('dimensions', root.nsmap)

    while dimensions:
        for d in dimensions:
            extent_copy.remove(d)
        dimensions = extent_copy.find('dimensions')

    locus = extent_copy.find('locus')

    while locus:
        extent_copy.remove(locus)
        locus = extent_copy.find('locus')

    clean_extent_copy: str = extent_copy.text

    try:
        copy_no_period = clean_extent_copy.replace('.', '')
        copy_no_space = copy_no_period.replace(' ', '')

        perfect_copy = copy_no_space.isdigit()

        if perfect_copy == True:
            folio_total = int(copy_no_space)
        else:
            folio_total = 0
            '''Is a total of folio given:'''
            given_total: int = clean_extent_copy.find("blöð alls")

            if given_total > 0:
                clean_extent_copy = clean_extent_copy[:given_total]
                folio_total = int(clean_extent_copy)
            else:  # I have no idea what this does
                '''No total is given. First Get rid of other unnecessary info:'''
                given_emptiness = clean_extent_copy.find("Auð blöð")
                if given_emptiness:
                    clean_extent_copy = clean_extent_copy[:given_emptiness]

                clean_extent_copy = re.sub(r"\([^()]*\)", "()", clean_extent_copy)

                brackets = clean_extent_copy.find("()")

                if brackets == -1:
                    folio_total = _get_digits(clean_extent_copy)

                else:
                    while brackets != -1:
                        first_bit: str = clean_extent_copy[:brackets]
                        clean_extent_copy = clean_extent_copy[brackets+2:]
                        brackets = clean_extent_copy.find("()")
                        folio_n = _get_digits(first_bit)
                        if folio_n:
                            folio_total = folio_total+folio_n

                    folio_z = _get_digits(extent_copy)
                    if folio_z:
                        folio_total = folio_total+folio_z

            folio_check: str = str(folio_total)
            if len(folio_check) > 3:
                name: str = tamer._find_full_id(root)
                log.warning(f"{name}: Attention. Check number of folios.")
                folio_total = 0

    except Exception:
        folio_total = 0
        log.warning(f"{folio_total}: Attention. Check number of folios.")

    return folio_total


# def get_dimensions(soup: BeautifulSoup) -> Tuple[int, int]:
#     """Get dimensions. If more than one volume, it calculates average dimensions. For quantitative usage.
#     Args:
#         soup (BeautifulSoup): BeautifulSoup

#     Returns:
#         tuple: height and width
#     """
#     extent: Tag = soup.find('extent')
#     if not extent:
#         return 0, 0
#     extent_copy = copy.copy(extent)
#     dimensions: Tag = extent_copy.find('dimensions')
#     height: Tag = []
#     width: Tag = []

#     myheights: List[int] = []
#     mywidths: List[int] = []
#     while dimensions:
#         height = dimensions.height
#         pretty_height: int = _get_length(height)

#         width = dimensions.width
#         pretty_width: int = _get_length(width)

#         myheights.append(pretty_height)
#         mywidths.append(pretty_width)

#         extent_copy.dimensions.unwrap()
#         dimensions = extent_copy.find('dimensions')

#     try:
#         perfect_height = sum(myheights) / len(myheights)
#         perfect_height = int(perfect_height)
#         perfect_width = sum(mywidths) / len(mywidths)
#         perfect_width = int(perfect_width)
#     except Exception:
#         log.debug("Something went wrong with the calculation of the dimensions.")
#         perfect_height = 0
#         perfect_width = 0

#     return perfect_height, perfect_width


# def _get_length(txt: str) -> int:
#     """Get length. If length is given as range, calculates average.

#     Args:
#         txt (str): txt with length info.

#     Returns:
#         int: returns length.
#     """

#     length: str = get_cleaned_text(txt)
#     try:
#         x: int = length.find("-")
#         if x == -1:
#             pretty_length: int = int(length)
#         else:
#             mylist = length.split("-")
#             int_map = map(int, mylist)
#             int_list = list(int_map)
#             pretty_length = int(sum(int_list) / len(int_list))
#             pretty_length = int(pretty_length)
#     except Exception:
#         log.info(f"Curr MS missing length!")
#         pretty_length = 0

#     return pretty_length


def get_extent(root: etree._Element) -> tuple[int, int, str]:  # TODO-BL: tidy up
    """Get extent of manuscript. For qualitative usage.
        NB! The 'extent' is the measurements of the leaves!

    Args:
        root (etree._Element): etree XML element object

    Returns:
        str: qualitative description of manuscript's extent
    """
    extent = root.find('.//extent', root.nsmap)

    if extent is None:
        return 0, 0, "no dimensions given"

    extent_copy = copy.copy(extent)

    dimensions = extent_copy.find('dimensions', root.nsmap)
    if dimensions is not None:
        try:
            height = dimensions.find("height", root.nsmap)
            width = dimensions.find("width", root.nsmap)

            height_measurements = 0
            if height is not None:
                try:
                    height_measurements = int(height.text)
                except Exception:
                    log.debug("There was a 'height' element, but getting text from it failed.")
            width_measurements = 0
            if width is not None:
                try:
                    width_measurements = int(width.text)
                except Exception:
                    log.debug("There was a 'width' element, but getting text from it failed.")
            unit = dimensions.get('unit')
            if unit is None:
                unit0 = height.get('unit')
                unit1 = width.get('unit')
                if unit0 is not None and unit1 is not None and unit0 == unit1:
                    unit = "mm"

            if width_measurements + height_measurements > 0:
                if not unit:
                    no_digits = len(str(width_measurements))
                    if no_digits == 3:
                        unit = "mm?"
                    if no_digits == 2:
                        unit = "cm?"
                    else:
                        unit = "??"
                pretty_extent = f"{height_measurements} x {width_measurements} {unit}"
            else:
                pretty_extent = "No dimensions given"
        except Exception:
            log.exception("failed building manuscript extent description")
            return 0, 0, "N/A"
        if not height:
            height = 0
        if not width:
            width = 0
        return height_measurements, width_measurements, pretty_extent
    else:
        log.exception("failed building manuscript extent description")
        return 0, 0, "N/A"


def get_description(root: etree._Element) -> tuple[str, str, str, str]:
    """Summarizes support and dimensions for usage in citavi.

    Args:
        root (etree._Element): etree XML element object

    Returns:
        str: support / dimensions
    """
    pretty_support = get_support(root)
    height, width, pretty_extent = get_extent(root)

    pretty_description = pretty_support + " / " + pretty_extent

    return str(height), str(width), pretty_extent, pretty_description


# def get_location(soup: BeautifulSoup) -> Tuple[str, str, str, str, str, str]:  # TODO: does that make some of the other funtions obsolete?
#     """Get data of the manuscript's location.

#     Args:
#         soup (bs4.BeautifulSoup): BeautifulSoup object

#     Returns:
#         tuple: data of manuscript's location
#     """
#     country = soup.country
#     try:
#         pretty_country = _get_key(country)
#         if not pretty_country:
#             pretty_country = get_cleaned_text(country)
#     except:
#         pretty_country = "Country unknown"

#     settlement = soup.find("settlement")
#     institution = soup.find("institution")
#     repository = soup.find("repository")
#     collection = soup.find("collection")
#     signature = soup.find("idno")

#     pretty_settlement = get_cleaned_text(settlement)
#     pretty_institution = get_cleaned_text(institution)
#     pretty_repository = get_cleaned_text(repository)
#     pretty_collection = get_cleaned_text(collection)
#     pretty_signature = get_cleaned_text(signature)

#     return pretty_country, pretty_settlement, pretty_institution, pretty_repository, pretty_collection, pretty_signature


def get_date(root: etree._Element) -> Tuple[str, int, int, int, int]:
    # TODO: Redesign /SK
    tag = root.find(".//origDate", root.nsmap)
    date = ""
    ta = 0
    tp = 0
    meandate = 0
    yearrange = 0
    if tag is None:
        return date, tp, ta, meandate, yearrange

    if tag.get("notBefore") is not None and tag.get("notAfter") is not None:
        notBefore = str(tag.attrib["notBefore"])
        notAfter = str(tag.attrib["notAfter"])

        # TODO: give indication why this happened
        # Snibbel Snibbel
        if len(notBefore) >= 5:
            notBefore = notBefore[0:4]

        if len(notAfter) >= 5:
            notAfter = notAfter[0:4]
        # Snibbel Snibbel Ende

        date = f"{notBefore}-{notAfter}"
        tp = int(notBefore)
        ta = int(notAfter)
        meandate = int(statistics.mean([int(tp), int(ta)]))
        yearrange = int(ta) - int(tp)

    elif tag.get("when"):
        date = str(tag.attrib["when"])
        normalized_date = date
        if len(normalized_date) > 4:
            normalized_date = normalized_date[:4]
        tp = int(normalized_date)
        ta = int(normalized_date)
        meandate = tp
        yearrange = 0

    elif tag.get("from") and tag.get("to"):
        fr = str(tag.attrib["from"])
        to = str(tag.attrib["to"])
        date = f"{fr}-{to}"
        n = fr
        if len(n) > 4:
            n = n[:4]
        tp = int(n)
        n = to
        if len(n) > 4:
            n = n[:4]
        ta = int(n)
        meandate = int(statistics.mean([int(tp), int(ta)]))
        yearrange = int(ta) - int(tp)

    return date, tp, ta, meandate, yearrange


def get_ms_id(root: etree._Element) -> Tuple[str, str, str]:
    ms_id = root.find(".teiHeader/fileDesc/sourceDesc/msDesc/msIdentifier", nsmap)

    if not ms_id:
        return "", "", ""
    else:
        co = ms_id.find("country", nsmap)
        try:
            country = co.text
        except:
            country = ""
        se = ms_id.find("settlement", nsmap)
        # settlement = se.text if se else ""
        # This should be working. This should result in settlement = se.text. But it doesnt. It ALWAYS fucking results in settlement = ""
        # WHY? /SK
        try:
            settlement = se.text
        except:
            settlement = ""
        re = ms_id.find("repository", nsmap)
        try:
            repository = re.text
        except:
            repository = ""
    return country, settlement, repository


def check_graphic(soup: BeautifulSoup) -> bool:
    graphic = soup.find('graphic')

    if graphic:
        g = True
    else:
        g = False

    return g
