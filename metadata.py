from typing import Dict, Generator, List, Tuple
import requests
import lxml
from bs4 import BeautifulSoup
from bs4.element import Tag
import urllib
import pandas as pd
from pandas import DataFrame
import copy
from crawler import load_xml
from crawler import load_xmls_by_id
from datetime import datetime
import re


# Test URLs
# ------------------
myURLList = ["https://handrit.is/en/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4925-is.xml"]
#myURLList = ["https://handrit.is/en/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/en/manuscript/xml/NKS04-1809-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4982-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4925-is.xml", "https://handrit.is/is/manuscript/xml/Lbs08-2296-is.xml", "https://handrit.is/is/manuscript/xml/JS04-0251-is.xml", "https://handrit.is/da/manuscript/xml/Acc-0001-da.xml", "https://handrit.is/is/manuscript/xml/Lbs02-0152-is.xml", "https://handrit.is/is/manuscript/xml/AM02-0197-en.xml", "https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/IB08-0165-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0110-I-II-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0048-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/AM04-1056-XVII-en.xml", "https://handrit.is/is/manuscript/xml/IB08-0174-is.xml", "https://handrit.is/is/manuscript/xml/AM02-0344-is.xml", "https://handrit.is/da/manuscript/xml/Acc-0001-da.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
#myURLList = ["https://handrit.is/en/manuscript/xml/Lbs04-0590-is.xml", "https://handrit.is/en/manuscript/xml/Acc-0036-en.xml", "https://handrit.is/is/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/en/manuscript/xml/NKS04-1809-is.xml", "https://handrit.is/is/manuscript/xml/Lbs08-2296-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
#myURLList = ["https://handrit.is/en/manuscript/xml/Lbs04-0590-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml", "https://handrit.is/en/manuscript/xml/AM02-0115-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml"]

# Constants
# ---------
_backspace_print = '                                     \r'

# Utlity Functions
# ----------------

""" Anmerkung: Folgender Funktion bedarf es noch für get_creator resp. get_persName. Ggf. streichen """
def get_soup(url: str) -> BeautifulSoup:
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
        return
    res = carrot.get_text()
    if not res:
        return
    res = res.replace('\n', ' ')
    res = res.replace('\t', ' ')
    res = ' '.join(res.split())

    return res
    
def get_structure(mylist: List[tuple], mytuple: Tuple[str]):
    """Adds tuple to list.

    Args:
        mylist (list): list containing tuples
        mytuple (tuple): tuple containing metadata

    Returns:
        list: mylist
    """
      
    mylist.append(mytuple)
    return mylist

def get_digits(text: str) -> int:
    """Gets digits from text.

    Args:
        text (str): text

    Returns:
        int: digits from text
    """
    i = ""
    for x in text:
        if x.isdigit():
            i += x

        else:
            pass    
    if i:
        i = int(i)
    else:
        i = 0

    return i


# Pull meta data
# ---------------

def get_tag(soup):
    tag = soup.msDesc
    handritID = str(tag['xml:id'])
    name = handritID[0:-3]
    return name

def get_key(leek: Tag) -> str:
    """Find key identifying the country and return country name.

    Args:
        leek (bs4.element.Tag): xml-tag

    Returns:
        str: country name
    """    

    key = leek.get('key')
    if key:
        if key == "IS":
            pretty_key = "Iceland"
        elif key == "DK":
            pretty_key = "Denmark"
        elif key == "FO":
            pretty_key = "Faroe Islands"
        else:
            pretty_key = "!! unknown country key"
            print("!! unknown country key. Fix function get_key!!")
    else:
        pretty_key = None

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
        pretty_origPlace = get_key(origPlace)
        if not pretty_origPlace:
            pretty_origPlace = get_cleaned_text(origPlace)    
    except:
        pretty_origPlace = "Origin unknown"

    return pretty_origPlace

def get_persName(id: str) -> str:
    """Get person name in nominative case.

    Args:
        id (str): identifier refering to a person

    Returns:
        str: person name
    """    
    
    url = "https://handrit.is/is/biography/xml/" + id
    stew = get_soup(url)
    persName = stew.find('persName')
    pretty_persName = get_cleaned_text(persName)
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
        creators = hands.find_all('name', {'type':'person'})

        if not creators: pretty_creators = "Scribe(s) unknown"
        else:
            pretty_creators = ""
            for creator in creators:
                key = creator.get('key')
                if key: pretty_creator = get_persName(key)
                else: pretty_creator = get_cleaned_text(creator)
                pretty_creators = pretty_creators + "; " + pretty_creator
            pretty_creators = pretty_creators[2:]
                
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
    if head: headtitle = head.find('title')
    summary = soup.summary
    if summary: summarytitle = summary.find('title')

    if msName: shorttitle = msName
    elif headtitle: shorttitle = headtitle   
    elif summarytitle: shorttitle = summarytitle
    else:
        msItem = soup.msItem
        shorttitle = msItem.find('title')        
        if not shorttitle:
            pretty_shorttitle = "None"
        else:
            pass
    try:
        pretty_shorttitle = get_cleaned_text(shorttitle)
    except:
        pass

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

    extent = soup.find('extent')
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
    
    folio_total: int = 0

    try:
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
            
            while brackets > 0:
                first_bit: str = clean_extent_copy[:brackets]         
                clean_extent_copy = clean_extent_copy[brackets+2:]
                brackets = clean_extent_copy.find("()")
                folio_n = get_digits(first_bit)
                if folio_n: folio_total = folio_total+folio_n

            folio_z = get_digits(extent_copy)
            if folio_z: folio_total = folio_total+folio_z

        folio_check: str = str(folio_total)
        if len(folio_check) > 3:
            name: str = get_tag(soup)
            print(name + ": Attention. Check number of folios.")
        elif folio_total == 0: folio_total = 0

    except:
        folio_total = 0

    return folio_total

def get_dimensions(soup: BeautifulSoup) -> tuple:
    """Get dimensions. If more than one volume, it calculates average dimensions. For quantitative usage.
    Args:
        soup (BeautifulSoup): BeautifulSoup

    Returns:
        tuple: height and width
    """

    extent: Tag = soup.find('extent')
    extent_copy = copy.copy(extent)
    dimensions: Tag = extent_copy.find('dimensions')
    height: Tag = []
    width: Tag = []

    myheights: list = []
    mywidths: list = []
    while dimensions:   
        height = dimensions.height
        pretty_height: int = get_length(height)
        
        width = dimensions.width
        pretty_width: int = get_length(width)

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

def get_length(txt: str) -> int:
    """Get length. If length is given as range, calculates average.

    Args:
        txt (str): txt with length info.

    Returns:
        int: returns length.
    """   

    length: str = get_cleaned_text(txt)
    x: int = length.find("-")
    if x == -1: pretty_length: int = int(length)
    else:
        mylist = length.split("-")
        int_map = map(int, mylist)
        int_list = list(int_map)
        pretty_length = sum(int_list) / len(int_list)
        pretty_length = int(pretty_length)

    return pretty_length

def get_extent(soup: BeautifulSoup) -> str:
    """Get extent of manuscript. For qualitative usage.

    Args:
        soup (BeautifulSoup): BeautifulSoup

    Returns:
        str: qualitative description of manuscript's extent
    """    

    extent: Tag = soup.find('extent')

    extent_copy = copy.copy(extent)

    dimensions: Tag = extent_copy.find('dimensions')

    while dimensions:        
     
        height: Tag = dimensions.height
        width: Tag = dimensions.width

        unit: str = dimensions.get('unit')
        if not unit: unit = "[mm?]"
        height.string = height.string + " x"
        width.string = width.string + " " + unit
        
        extent_copy.dimensions.unwrap()
        dimensions = extent_copy.find('dimensions')

    pretty_extent: str = get_cleaned_text(extent_copy)

    if not pretty_extent:
        pretty_extent = "no dimensions given"

    return pretty_extent




def get_description(soup):
    """Summarizes support and dimensions for usage in citavi.

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        str: support / dimensions
    """    

    pretty_support = get_support(soup)
    pretty_extent = get_extent(soup)
   
    pretty_description = pretty_support + " / "  + pretty_extent

    return pretty_description


def get_location(soup: BeautifulSoup) -> tuple[str, str, str, str, str]:
    """Get data of the manuscript's location.

    Args:
        soup (bs4.BeautifulSoup): BeautifulSoup object

    Returns:
        tuple: data of manuscript's location
    """    
    country = soup.country
    try:
        pretty_country = get_key(country)
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


# Get all metadata
# ----------------

def get_all_data(links: list) -> tuple:    
    """ Create dataframe for usage in interface

    The dataframe contains the following collumns:
    ...

    Args:
        links (list): list of urls

    Returns:
        tuple: pd.DataFrame (containing manuscript meta data), file_name
    """    
    
    mylist = []
     
    i = 0
    for url in links:

        print(f'Loading: {i}', end=_backspace_print)
        i += 1

        soup = load_xml(url)

        #soup = load_xmls_by_id(id)
        #gets soup from url (without crawler)
        #soup = get_soup(url)

        name = get_tag(soup)
        location = get_location(soup)
        shorttitle = get_shorttitle(soup)
        creator = get_creator(soup)    
        dimensions = get_dimensions(soup)
        folio = get_folio(soup)
        origin = get_origin(soup)
        support = get_support(soup)

        mytuple = (name,) + (creator,) + (shorttitle,) + (origin,) + location + (support,) + dimensions + (folio,)
        structure = get_structure(mylist, mytuple) 

        columns = ["Handrit-ID", "Creator", "Short title", "Origin", "Country", "Settlement", "Institution", "Repository", "Collection", "Signature", "Support", "Height", "Width", "Folio"]
        data = pandafy_data(structure, columns)

        file_name = "metadata"
    
    print(f'Loaded:  {i}',)

    return data, file_name


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

def get_citavified_data (links: list) -> tuple:  
    """ Create dataframe for usage in interface

    The dataframe contains the following collumns:
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
        links (list): list of urls

    Returns:
        tuple: pd.DataFrame (containing manuscript meta data), file_name
    """

    i = 0

    mylist = []
    for url in links:
        print(f'Loading: {i}', end=_backspace_print)
        i += 1

        soup = load_xml(url)
        #soup = load_xmls_by_id(id)

        name = get_tag(soup)    
        creator = get_creator(soup)  
        shorttitle = get_shorttitle(soup)
        description = get_description(soup)
        date = "Eline?"
        origin = get_origin(soup)
        location = get_location(soup)
        settlement, archive, signature = summarize_location(location)
        
        mytuple = (name,) + (creator,) + (shorttitle,) + (description,) + (date,) + (origin,) + (settlement,) + (archive,) + (signature,)
        structure = get_structure(mylist, mytuple) 

    columns = ["Handrit-ID", "Creator", "Short title", "Description", "Dating", "Origin",  "Settlement", "Archive", "Signature"]

    data = pandafy_data(structure, columns)
    file_name = "metadata_citavified"

    print(f'Loaded:  {i}',)

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
        copies= [copy.copy(item) for item in items]
        cleaned_copies = clean_msitems(copies)
        structure = dictionarize(cleaned_copies)
        yield url, structure


# Pandas and CSV export
# ---------------------

def pandafy_data(result: list, columns: list[str]) -> DataFrame:
    """Creates panda dataframe.

    Args:
        result (list): list of metadata
        columns (list): list of columns description

    Returns:
        pandas.core.frame.DataFrame: panda dataframe
    """    

    data = []
    data = pd.DataFrame(result)
    data.columns = columns

    return data

def CSVExport(FileName: str, DataFrame):
    DataFrame.to_csv(FileName+".csv", sep ='\t', encoding='utf-8', index=False)
    print("File exported")
    return


# Test Runner
# -----------

if __name__ == "__main__":
    print("Test Runner:")
    print("------------")
    print(f'Start: {datetime.now()}')

    '''Hier zwei Funktionen zum Citavi-freundliches Data oder alles Data zu bekommen (unten der Export)'''

    # Get data for citavi
    # -----------------------
    data_c, file_name_c = get_citavified_data(myURLList)

    # Get all data
    # ----------------
    #data, file_name = get_all_data(myURLList) 


    '''Hier ist noch Balduins Titel-Finder. Allerdings noch nicht in einem richtigen Dataframe oder dergleichen!'''
    # Get all titles
    # --------------
    #list_of_results = do_it_my_way(myURLList)
    #for url, result in list_of_results:
    #    print(url)
    #    for vals in result:
    #        print(vals)
    #    print('\n------------------\n')
    

    # CSV exports
    # ----------
    CSVExport(file_name_c, data_c)
    #CSVExport(file_name, data)

    print(f'Finished: {datetime.now()}')
    print("------------")