import copy
import re
from typing import List, Optional, Tuple

from lxml import etree

import lib.xml.tamer as tamer
from lib import utils

log = utils.get_logger(__name__)
nsmap = {None: "http://www.tei-c.org/ns/1.0", 'xml': 'http://www.w3.org/XML/1998/namespace'}


# Utlity Functions
# ----------------


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
    # TODO: look into this method... can this be streamlined? it also picks up lots of errors...
    extent: etree._Element = root.find('.//extent', root.nsmap)

    if extent is None:
        return 0
    extent_copy = copy.copy(extent)

    dimensions = extent_copy.findall('dimensions', root.nsmap)

    while dimensions:
        for d in dimensions:
            extent_copy.remove(d)
        dimensions = extent_copy.find('dimensions', root.nsmap)

    locus = extent_copy.find('locus', root.nsmap)

    while locus:
        extent_copy.remove(locus)
        locus = extent_copy.find('locus', root.nsmap)

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

                    folio_z = _get_digits(clean_extent_copy)
                    if folio_z:
                        folio_total = folio_total+folio_z

            folio_check: str = str(folio_total)
            if len(folio_check) > 3:
                name: str = tamer._find_full_id(root)
                log.warning(f"{name}: Attention. Check number of folios: {len(folio_check)} ({folio_check}) from: {extent_copy.text}")
                if extent_copy.items():
                    log.error(f"Note: extent had attributes: {extent_copy.items()}")
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

    dimensions = extent.find('dimensions', root.nsmap)
    if dimensions is None:
        log.debug("failed building manuscript extent description")
        return 0, 0, "N/A"
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
        if unit is not None:
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
    if height is None:
        height = 0
    if width is None:
        width = 0
    return height_measurements, width_measurements, pretty_extent


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
