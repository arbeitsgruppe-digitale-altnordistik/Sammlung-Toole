from typing import List, Optional, Tuple
from bs4 import BeautifulSoup
from bs4.element import Tag
import urllib
import pandas as pd
from pandas import DataFrame
import copy
import re
import statistics
from util import utils
from util.constants import PERSON_CACHE_PATH as people
import csv


log = utils.get_logger(__name__)


# Utlity Functions
# ----------------


""" Anmerkung: Folgender Funktion bedarf es noch für get_creator resp. get_persName. Ggf. streichen """  # QUESTION: what?


def get_soup(url: str) -> BeautifulSoup:  # TODO: should become obsolete
    """Get a BeautifulSoup object from a URL

    Args:
        url (str): url

    Returns:
        bs4.BeautifulSoup: BeautifulSoup object representation of the HTML/XML page.
    """

    sauce = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(sauce, "xml")

    return soup


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


# def get_structure(mylist: List[tuple], mytuple: Tuple[str]):  # TODO: might get obsolete?
#     """Adds tuple to list.

#     Args:
#         mylist (list): list containing tuples
#         mytuple (tuple): tuple containing metadata

#     Returns:
#         list: mylist
#     """
#     mylist.append(mytuple)
#     return mylist


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

        else:
            pass
    if s:
        i = int(s)
    else:
        i = 0

    return i


# Pull meta data
# ---------------


# def get_tag(soup):  # TODO: should become obsolete
#     tag = soup.msDesc
#     handritID = str(tag['xml:id'])
#     name = handritID[0:-3]
#     return name


def _get_key(leek: Tag) -> Optional[str]:
    """Find key identifying the country and return country name.

    Args:
        leek (bs4.element.Tag): xml-tag

    Returns:
        str: country name
    """
    key = leek.get('key')
    if key:
        if key == "IS" or key == "is":
            pretty_key = "Iceland"
        elif key == "DK" or key == "dk":
            pretty_key = "Denmark"
        elif key == "FO" or key == "fo":
            pretty_key = "Faroe Islands"
        elif key == "NO" or key == "no":
            pretty_key = "Norway"
        elif key == "SE" or key == "se":
            pretty_key = "Sweden"
        elif key == "KA" or key == "ka":
            pretty_key = "Canada"
        else:
            pretty_key = "!! unknown country key"
            log.warning(f"unknown country key: {key}. (Fix function get_key)")
    else:
        return None

    return pretty_key


def get_origin(soup: BeautifulSoup) -> str:
    """Get manuscript's place of origin.

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        str: country name
    """
    origPlace = soup.origPlace

    try:
        pretty_origPlace = _get_key(origPlace)
        if not pretty_origPlace:
            pretty_origPlace = get_cleaned_text(origPlace)
    except:
        pretty_origPlace = "Origin unknown"

    return pretty_origPlace


def _get_persName(id: str) -> str:  # LATER: improved person handling anyways
    """Get person name in nominative case.

    Args:
        id (str): identifier refering to a person

    Returns:
        str: person name
    """
    cachedFolks = {}
    with open(people, 'r', encoding='UTF-8') as infile:
        reader = csv.reader(infile)
        cachedFolks = {rows[0]: rows[1] for rows in reader}
    if id in cachedFolks:
        return cachedFolks[id]
    else:
        url = "https://handrit.is/is/biography/xml/" + id
        stew = get_soup(url)
        persName = stew.find('persName')
        pretty_persName = get_cleaned_text(persName)
        with open(people, 'a', encoding='UTF-8') as outfile:
            outfile.write(f"\n{id},{pretty_persName}")
        return pretty_persName


def get_creator(soup: BeautifulSoup) -> str:
    """Get creator(s).

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        str: creator name(s)
    """
    hands = soup.handDesc

    if hands:
        try:
            creators = hands.find_all('name', {'type': 'person'})

            if not creators:
                # LATER: find out why, if that happens
                pretty_creators = "Scribe(s) unknown"
            else:
                pretty_creators = ""
                for creator in creators:
                    key = creator.get('key')
                    if key:
                        pretty_creator = _get_persName(key)
                    else:
                        pretty_creator = get_cleaned_text(creator)
                    pretty_creators = pretty_creators + "; " + pretty_creator
                pretty_creators = pretty_creators[2:]
        except:
            # LATER: find out why, if that happens
            pretty_creators = "Scribe(s) unknown"
    else:
        pretty_creators = "Scribe(s) unknown"

    return pretty_creators


def get_shorttitle(soup: BeautifulSoup) -> str:
    """Get short title to describe (NOT identify) a manuscript.

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        str: short title
    """
    msName = ""
    headtitle = ""
    summarytitle = ""

    msName = soup.find('msName')
    head = soup.head
    if head:
        headtitle = head.find('title')
    summary = soup.summary
    if summary:
        summarytitle = summary.find('title')

    if msName:
        shorttitle = msName
    elif headtitle:
        shorttitle = headtitle
    elif summarytitle:
        shorttitle = summarytitle
    else:
        msItem = soup.msItem
        if not msItem:
            return "None"
        shorttitle = msItem.find('title')
        if not shorttitle:
            pretty_shorttitle = "None"
    try:
        pretty_shorttitle = get_cleaned_text(shorttitle)
    except:
        return shorttitle

    return pretty_shorttitle


def get_support(soup: BeautifulSoup) -> str:
    """Get supporting material (paper or parchment).

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        str: supporting material
    """
    supportDesc = soup.find('supportDesc')
    if supportDesc:
        support = supportDesc.get('material')
        if support == "chart":
            pretty_support = "Paper"
        elif support == "perg":
            pretty_support = "Parchment"
        else:
            pretty_support = support
            try:
                pretty_support = get_cleaned_text(support)
            except:
                pretty_support = ""
    else:
        pretty_support = ""

    return pretty_support


def get_folio(soup: BeautifulSoup) -> int:
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
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        int: total of folios
    """
    # TODO: look into this method... can this be streamlined?
    extent = soup.find('extent')
    if not extent:
        return 0
    extent_copy = copy.copy(extent)

    dimensions = extent_copy.find('dimensions')

    while dimensions:
        dimensions.decompose()
        dimensions = extent_copy.find('dimensions')

    locus = extent_copy.find('locus')

    while locus:
        locus.decompose()
        locus = extent_copy.find('locus')

    clean_extent_copy: str = get_cleaned_text(extent_copy)

    try:
        copy_no_period = clean_extent_copy.replace('.', '')
        copy_no_space = copy_no_period.replace(' ', '')

        perfect_copy = copy_no_space.isdigit()

        if perfect_copy == True:
            folio_total = copy_no_space

        else:
            folio_total: int = 0
            '''Is a total of folio given:'''
            given_total: int = clean_extent_copy.find("blöð alls")

            if given_total > 0:
                clean_extent_copy = clean_extent_copy[:given_total]
                folio_total = int(clean_extent_copy)
            else:
                '''No total is given. First Get rid of other unnecessary info:'''
                given_emptiness: str = clean_extent_copy.find("Auð blöð")
                if given_emptiness > 0:
                    clean_extent_copy = clean_extent_copy[:given_emptiness]
                else:
                    pass

                clean_extent_copy = re.sub(r"\([^()]*\)", "()", clean_extent_copy)

                brackets: str = clean_extent_copy.find("()")

                if brackets == -1:
                    folio_total = _get_digits(clean_extent_copy)

                else:
                    while brackets > 0:
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
                name: str = get_tag(soup)
                log.warning(f"{name}: Attention. Check number of folios.")
            elif folio_total == 0:
                folio_total = 0
            else:
                pass

    except:
        folio_total = 0
        log.warning(f"{folio_total}: Attention. Check number of folios.")

    return folio_total


def get_dimensions(soup: BeautifulSoup) -> Tuple[int, int]:
    """Get dimensions. If more than one volume, it calculates average dimensions. For quantitative usage.
    Args:
        soup (BeautifulSoup): BeautifulSoup

    Returns:
        tuple: height and width
    """
    extent: Tag = soup.find('extent')
    if not extent:
        return 0, 0
    extent_copy = copy.copy(extent)
    dimensions: Tag = extent_copy.find('dimensions')
    height: Tag = []
    width: Tag = []

    myheights: List[int] = []
    mywidths: List[int] = []
    while dimensions:
        height = dimensions.height
        pretty_height: int = _get_length(height)

        width = dimensions.width
        pretty_width: int = _get_length(width)

        myheights.append(pretty_height)
        mywidths.append(pretty_width)

        extent_copy.dimensions.unwrap()
        dimensions = extent_copy.find('dimensions')

    try:
        perfect_height = sum(myheights) / len(myheights)
        perfect_height = int(perfect_height)
        perfect_width = sum(mywidths) / len(mywidths)
        perfect_width = int(perfect_width)
    except:
        perfect_height = 0
        perfect_width = 0

    return perfect_height, perfect_width


def _get_length(txt: str) -> int:
    """Get length. If length is given as range, calculates average.

    Args:
        txt (str): txt with length info.

    Returns:
        int: returns length.
    """

    length: str = get_cleaned_text(txt)
    try:
        x: int = length.find("-")
        if x == -1:
            pretty_length: int = int(length)
        else:
            mylist = length.split("-")
            int_map = map(int, mylist)
            int_list = list(int_map)
            pretty_length = int(sum(int_list) / len(int_list))
            pretty_length = int(pretty_length)
    except:
        log.info(f"Curr MS missing length!")
        pretty_length = 0

    return pretty_length


def get_extent(soup: BeautifulSoup) -> str:
    """Get extent of manuscript. For qualitative usage.

    Args:
        soup (BeautifulSoup): BeautifulSoup

    Returns:
        str: qualitative description of manuscript's extent
    """
    extent: Tag = soup.find('extent')

    if not extent:
        return "no dimensions given"

    extent_copy = copy.copy(extent)

    dimensions: Tag = extent_copy.find('dimensions')

    try:
        while dimensions:

            height: Tag = dimensions.height
            width: Tag = dimensions.width

            unit: str = dimensions.get('unit')
            if not unit:
                unit = "[mm?]"
            height.string = height.string + " x"
            width.string = width.string + " " + unit

            extent_copy.dimensions.unwrap()
            dimensions = extent_copy.find('dimensions')
    except Exception:
        log.exception("failed building manuscript extent description")
        return "N/A"

    pretty_extent: str = get_cleaned_text(extent_copy)

    if not pretty_extent:
        pretty_extent = "no dimensions given"

    return pretty_extent


def get_description(soup: BeautifulSoup) -> str:
    """Summarizes support and dimensions for usage in citavi.

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        str: support / dimensions
    """
    pretty_support = get_support(soup)
    pretty_extent = get_extent(soup)

    pretty_description = pretty_support + " / " + pretty_extent

    return pretty_description


def get_location(soup: BeautifulSoup) -> Tuple[str, str, str, str, str, str]:  # TODO: does that make some of the other funtions obsolete?
    """Get data of the manuscript's location.

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        tuple: data of manuscript's location
    """
    country = soup.country
    try:
        pretty_country = _get_key(country)
        if not pretty_country:
            pretty_country = get_cleaned_text(country)
    except:
        pretty_country = "Country unknown"

    settlement = soup.find("settlement")
    institution = soup.find("institution")
    repository = soup.find("repository")
    collection = soup.find("collection")
    signature = soup.find("idno")

    pretty_settlement = get_cleaned_text(settlement)
    pretty_institution = get_cleaned_text(institution)
    pretty_repository = get_cleaned_text(repository)
    pretty_collection = get_cleaned_text(collection)
    pretty_signature = get_cleaned_text(signature)

    return pretty_country, pretty_settlement, pretty_institution, pretty_repository, pretty_collection, pretty_signature


def get_date(soup: BeautifulSoup) -> Tuple[str, int, int, int, int]:
    tag = soup.origDate
    date = ""
    ta = 0
    tp = 0
    meandate = 0
    yearrange = 0
    if not tag:
        return date, tp, ta, meandate, yearrange

    if tag.get("notBefore") and tag.get("notAfter"):
        notBefore = str(tag["notBefore"])
        notAfter = str(tag["notAfter"])

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
        date = str(tag["when"])
        normalized_date = date
        if len(normalized_date) > 4:
            normalized_date = normalized_date[:4]
        tp = int(normalized_date)
        ta = int(normalized_date)
        meandate = tp
        yearrange = 0

    elif tag.get("from") and tag.get("to"):
        fr = str(tag["from"])
        to = str(tag["to"])
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


def get_msID(soup: BeautifulSoup) -> Tuple[str, str, str, str]:
    msID = soup.find("msIdentifier")
    if not msID:
        return "", "", "", ""
    else:
        c = msID.find("country")
        country: str = c.get_text() if c else ""
        s = msID.find("settlement")
        settlement: str = s.get_text() if s else ""
        r = msID.find("repository")
        repository: str = r.get_text() if r else ""
        si = msID.find("idno")
        signature: str = si.get_text() if si else ""
    return signature, country, settlement, repository


def check_graphic(soup: BeautifulSoup) -> bool:
    graphic = soup.find('graphic')

    if graphic:
        g = True
    else:
        g = False

    return g

# Get all metadata
# ----------------


# def get_all_data(inData: List[str], DataType: str = 'urls') -> Tuple[pd.DataFrame, str]:    # TODO: should become obsolete when these methods are called from handler
#     """ Create dataframe for usage in interface

#     The dataframe contains the following collumns:
#     "Handrit-ID", "Creator", "Short title", "Origin", "Country", "Settlement", "Institution", "Repository", "Collection", "Signature", "Support", "Height", "Width", "Folio"

#     Args:
#         inData (list): list of urls or ids
#         DataType: Whether its a list of URLs or IDs. Allowed: 'urls', 'ids'

#     Returns:
#         tuple: pd.DataFrame (containing manuscript meta data), file_name
#     """

#     mylist = []

#     i = 0
#     for thing in inData:

#         log.info(f'Loading: {i}, which is {thing}')
#         i += 1
#         if DataType == 'urls':
#             soup = load_xml(thing)

#         if DataType == 'ids':
#             presoup = load_xmls_by_id(thing)
#             soup = list(presoup.values())[0]  # TODO: do we really want to hard-exclude multi language like this?

#         # gets soup from url (without crawler)
#         # soup = get_soup(url)

#         name = get_tag(soup)
#         location = get_location(soup)
#         shorttitle = get_shorttitle(soup)
#         creator = get_creator(soup)
#         dimensions = get_dimensions(soup)
#         folio = get_folio(soup)
#         origin = get_origin(soup)
#         support = get_support(soup)
#         graphic = check_graphic(soup)
#         dates = get_date(soup)

#         mytuple = (name,) + (creator,) + (shorttitle,) + (origin,) + location + (support,) + dimensions + (folio,) + dates + (graphic,)
#         structure = get_structure(mylist, mytuple)

#         columns = ["Handrit-ID", "Creator", "Short title", "Origin", "Country", "Settlement",
#                    "Institution", "Repository", "Collection", "Signature", "Support", "Height", "Width", "Folio",
#                    "Date", "Tempus post quem", "Tempus ante quem", "Mean Date", "Year Range", "Digitized"]
#         data = pandafy_data(structure, columns)

#     log.info(f'Loaded:  {i}',)

#     return data


# Get metadata and citavify
# -------------------------


def summarize_location(location: Tuple[str, str, str, str, str, str]) -> Tuple[str, str, str]:
    """ Get manuscript location and summarize for usage in citavi

    Args:
        location (tuple): metadata of manuscript's location

    Returns:
        tuple: summary of metadata of manuscript's location
    """

    location_list = list(location)
    for i in range(len(location_list)):
        if not location_list[i]:
            location_list[i] = ""
            continue
        location_list[i] = location_list[i] + ", "
    try:
        settlement = location_list[1] + location_list[0]
        settlement = settlement[:-2]

    except:
        settlement = "unknown"

    try:
        archive = location_list[2] + location_list[3] + location_list[4]
        archive = archive[:-2]
    except:
        archive = "unknown"

    signature = location_list[5]
    signature = signature[:-2]

    return settlement, archive, signature


def get_citavified_data(inData: List[str], DataType: str = 'urls') -> Tuple[pd.DataFrame, str]:  # XXX: get this to work
    """ Create dataframe for usage in interface

    The dataframe contains the following columns:
    - Handrit ID (`Handrit-ID`)
    - Creators / scribes (`Creator`)
    - Short title (`Short title`)
    - Description (`Description`)
    - Dating (`Dating`)
    - Manusript origin (`Origin`)
    - Place of archive (`Settlement`)
    - Name of archive (`Archive`)
    - Signature (`Signature`)

    Args:
        inData (list): list of urls or ids
        DataType (str): Whether the list consists of URLs or IDs. Allowed: 'urls', 'ids'

    Returns:
        tuple: pd.DataFrame (containing manuscript meta data), file_name
    """

    i = 0

    mylist = []
    for thing in inData:
        log.info(f'Loading: {i}')
        i += 1

        if DataType == 'urls':
            soup = load_xml(thing)
        if DataType == 'ids':
            presoup = load_xmls_by_id(thing)
            soup = list(presoup.values())[0]

        name = get_tag(soup)
        creator = get_creator(soup)
        shorttitle = get_shorttitle(soup)
        description = get_description(soup)
        dates = get_date(soup)
        date = dates[1]
        origin = get_origin(soup)
        location = get_location(soup)
        settlement, archive, signature = summarize_location(location)

        mytuple = (name,) + (creator,) + (shorttitle,) + (description,) + (date,) + (origin,) + (settlement,) + (archive,) + (signature,)
        structure = get_structure(mylist, mytuple)

    columns = ["Handrit-ID", "Creator", "Short title", "Description", "Dating", "Origin",  "Settlement", "Archive", "Signature"]

    data = pandafy_data(structure, columns)
    file_name = "metadata_citavified"

    log.info(f'Loaded:  {i}',)

    return data, file_name


# Get titles (by Balduin)
# -----------------------

def clean_msitems(soups):
    for soup in soups:
        for sub in soup.find_all('msItem'):
            sub.decompose()
        yield soup


def get_title(item):
    title = item.find('title')
    rubric = item.find('rubric')
    return title, rubric


def dictionarize(items):
    res = []
    for item in items:
        number = item.get('n')
        title, rubric = get_title(item)
        pretty_title = get_cleaned_text(title)
        pretty_rubric = get_cleaned_text(rubric)
        res.append((number, pretty_title, pretty_rubric,))
    return res


def do_it_my_way(links):
    for url in links:
        soup = get_soup(url)
        items = soup.find_all('msItem')
        copies = [copy.copy(item) for item in items]
        cleaned_copies = clean_msitems(copies)
        structure = dictionarize(cleaned_copies)
        yield url, structure


# Pandas and CSV export
# ---------------------


def pandafy_data(result: list, columns: list) -> DataFrame:
    """Creates panda dataframe.

    Args:
        result (list): list of metadata
        columns (list): list of columns description

    Returns:
        pandas.core.frame.DataFrame: panda dataframe
    """
    data = pd.DataFrame(result)
    data.columns = columns

    return data


def CSVExport(FileName: str, DataFrame: pd.DataFrame) -> None:
    DataFrame.to_csv(FileName+".csv", sep='\t', encoding='utf-8', index=False)
    log.info("File exported")


# Test Runner
# -----------


# Test URLs
# myURLList = ["https://handrit.is/en/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4925-is.xml"]
# myURLList = ["https://handrit.is/en/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/en/manuscript/xml/NKS04-1809-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4982-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4925-is.xml", "https://handrit.is/is/manuscript/xml/Lbs08-2296-is.xml", "https://handrit.is/is/manuscript/xml/JS04-0251-is.xml", "https://handrit.is/da/manuscript/xml/Acc-0001-da.xml", "https://handrit.is/is/manuscript/xml/Lbs02-0152-is.xml", "https://handrit.is/is/manuscript/xml/AM02-0197-en.xml", "https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/IB08-0165-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0110-I-II-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0048-is.xml"]
# myURLList = ["https://handrit.is/is/manuscript/xml/AM04-1056-XVII-en.xml", "https://handrit.is/is/manuscript/xml/IB08-0174-is.xml", "https://handrit.is/is/manuscript/xml/AM02-0344-is.xml", "https://handrit.is/da/manuscript/xml/Acc-0001-da.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
# myURLList = ["https://handrit.is/en/manuscript/xml/Lbs04-0590-is.xml", "https://handrit.is/en/manuscript/xml/Acc-0036-en.xml", "https://handrit.is/is/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/en/manuscript/xml/NKS04-1809-is.xml", "https://handrit.is/is/manuscript/xml/Lbs08-2296-is.xml"]
# myURLList = ["https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
# myURLList = ["https://handrit.is/en/manuscript/xml/Lbs04-0590-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml", "https://handrit.is/en/manuscript/xml/AM02-0115-is.xml"]
# myURLList = ["https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml"]
# myURLList = ['AM02-0002', 'AM02-0022', 'AM02-190-b']


# if __name__ == "__main__":
#     print("Test Runner:")
#     print("------------")
#     print(f'Start: {datetime.now()}')

#     '''Hier zwei Funktionen zum Citavi-freundliches Data oder alles Data zu bekommen (unten der Export)'''

#     # Get data for citavi
#     # -----------------------
#     data_c, file_name_c = get_citavified_data(myURLList)

#     # Get all data
#     # ----------------
#     # data = get_all_data(myURLList, DataType='ids')
#     # print(data)

#     '''Hier ist noch Balduins Titel-Finder. Allerdings noch nicht in einem richtigen Dataframe oder dergleichen!'''
#     # Get all titles
#     # --------------
#     #list_of_results = do_it_my_way(myURLList)
#     # for url, result in list_of_results:
#     #    print(url)
#     #    for vals in result:
#     #        print(vals)
#     #    print('\n------------------\n')

#     # CSV exports
#     # ----------
#     # CSVExport(file_name_c, data_c)
#     # CSVExport(file_name, data)

#     print(f'Finished: {datetime.now()}')
#     print("------------")
