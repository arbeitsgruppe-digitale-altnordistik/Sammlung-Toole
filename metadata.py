import requests
import lxml
from bs4 import BeautifulSoup
import urllib
import pandas as pd
import copy
from crawler import load_xml
from crawler import load_xmls_by_id

import re


#myURLList = ["https://handrit.is/en/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/en/manuscript/xml/NKS04-1809-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4982-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-4925-is.xml", "https://handrit.is/is/manuscript/xml/Lbs08-2296-is.xml", "https://handrit.is/is/manuscript/xml/JS04-0251-is.xml", "https://handrit.is/da/manuscript/xml/Acc-0001-da.xml", "https://handrit.is/is/manuscript/xml/Lbs02-0152-is.xml", "https://handrit.is/is/manuscript/xml/AM02-0197-en.xml", "https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/IB08-0165-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0110-I-II-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0048-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/AM04-1056-XVII-en.xml", "https://handrit.is/is/manuscript/xml/IB08-0174-is.xml", "https://handrit.is/is/manuscript/xml/AM02-0344-is.xml", "https://handrit.is/da/manuscript/xml/Acc-0001-da.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
#myURLList = ["https://handrit.is/en/manuscript/xml/Lbs04-0590-is.xml", "https://handrit.is/en/manuscript/xml/Acc-0036-en.xml", "https://handrit.is/is/manuscript/xml/AM02-0115-is.xml", "https://handrit.is/en/manuscript/xml/NKS04-1809-is.xml", "https://handrit.is/is/manuscript/xml/Lbs08-2296-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
myURLList = ["https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml"]


# Dieser Funktion bedarf es noch für die Schreiber-Namen
def get_soup(url):
    sauce = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(sauce, "xml")
    return soup

def get_cleaned_text(soup):
    if not soup:
        return
    res = soup.get_text()
    if not res:
        return
    res = res.replace('\n', ' ')
    res = res.replace('\t', ' ')
    res = ' '.join(res.split())
    return res
    
def get_structure(mylist, mytuple):
    mylist.append(mytuple)
    return mylist

# country key => country name
def get_key(mirepoix):
    key = mirepoix.get('key')
    if key:
        if key == "IS":
            pretty_key = "Iceland"
        elif key == "DK":
            pretty_key = "Denmark"
        elif key == "FO":
            pretty_key = "Faroe Islands"
        else:
            pretty_key = "!! unknown country key"
    else:
        pretty_key = None

    return pretty_key


def get_origin(soup):
    origPlace = soup.origPlace

    try:
        pretty_origPlace = get_key(origPlace)
        if not pretty_origPlace:
            pretty_origPlace = get_cleaned_text(origPlace)    
    except:
        pretty_origPlace = "Origin unknown"

    return pretty_origPlace


def get_persName(id):
    url = "https://handrit.is/is/biography/xml/" + id
    stew = get_soup(url)
    persName = stew.find('persName')
    pretty_persName = get_cleaned_text(persName)
    return pretty_persName

def get_creator(soup):
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

def get_shorttitle(soup):
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

def get_support(soup):
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
        
    
    return(pretty_support)

def get_folio(soup):

    extent = soup.find('extent')
    extent_copy = copy.copy(extent)

    # get rid of info that might bias number of folio
    dimensions = extent_copy.find('dimensions')

    while dimensions:
        dimensions.decompose()
        dimensions = extent_copy.find('dimensions')

    locus = extent_copy.find('locus')

    while locus:
        locus.decompose()
        locus = extent_copy.find('locus')

    # get folio
    extent_copy = get_cleaned_text(extent_copy)

    try:
        # is total of folio given?
        given_total = extent_copy.find("blöð alls")
        if given_total > 0:
            extent_copy = extent_copy[:given_total]
            folio_total = get_digits(extent_copy)

        else:
            # get rid of pages refering to empty pages
            given_emptiness = extent_copy.find("Auð blöð")
            if given_emptiness > 0:
                extent_copy = extent_copy[:given_emptiness]
            else:
                pass
            
            # get rid of further unnecessary info about the ms in brackets, possibly containing digits
            extent_copy = re.sub(r"\([^()]*\)", "()", extent_copy)

            brackets = extent_copy.find("()")
            folio_total = 0
            while brackets > 0:
                first_bit= extent_copy[:brackets]           
                extent_copy = extent_copy[brackets+2:]
                brackets = extent_copy.find("()")
                folio_n = get_digits(first_bit)
                if folio_n: folio_total = folio_total+folio_n

            folio_z = get_digits(extent_copy)
            if folio_z: folio_total = folio_total+folio_z

        # check whether calculated number of folio is reasonable (< 1.000) - add [?] if not -
        # or whether it is 0 - write n/a.

        folio_check = str(folio_total)
        if len(folio_check) > 3: folio_total = str(folio_total) + " [?]"
        elif folio_total == 0: folio_total = "n/a"

    except:
        folio_total = "n/a"
    
    return(folio_total)

def get_digits(text):
    i = ""

    for x in text:
        if x.isdigit():
            i += x
        else:
            pass    
    if i:
        i = int(i)
    else:
        i = ""
    return(i)


def get_extent(soup):
    extent = soup.find('extent')

    extent_copy = copy.copy(extent)

    dimensions = extent_copy.find('dimensions')


    while dimensions:        
     
        height = dimensions.height
        width = dimensions.width

        unit = dimensions.get('unit')
        if not unit: unit = "[mm?]"
        height.string = height.string + " x"
        width.string = width.string + " " + unit
        
        extent_copy.dimensions.unwrap()
        dimensions = extent_copy.find('dimensions')

    pretty_extent = get_cleaned_text(extent_copy)

    if not pretty_extent:
        pretty_extent = "no dimensions given"
    
    return(pretty_extent)


# ÜBERARBEITEN: get_dimensions
def get_dimensions(soup):
    extent = soup.find('extent')

    # Gibt Fall, dass extent/
    extent_check = get_cleaned_text(extent)

    #if extent:
    #
    if extent_check:
        try:
            dimensions = soup.dimensions
            
            # Problem: Funktioniert nur erfolgreich, wenn nur einmal Maße angegeben werden! Zudem
            # klaut es auch die Maße aus einem Fließtext, sofern dort eingebunden.
            # wäre es interessant, die Maße als int zu ziehen anstatt str für etwaige Erhebung?

            height = dimensions.find('height')
            width = dimensions.find('width')
            unit = dimensions.get('unit')
            if not unit: unit = "mm (?)"
            pretty_height = get_cleaned_text(height)
            pretty_width = get_cleaned_text(width)
            pretty_dimensions = pretty_height + " x " + pretty_width + " " + unit
            dimensions.decompose()
        except:
            pretty_dimensions = ""
        
        pretty_extension = get_cleaned_text(extent)
    else:
        pretty_extent = ""
        pretty_dimensions = ""
    
    return(pretty_extent)

def get_description(soup):
    pretty_support = get_support(soup)
    pretty_extent = get_extent(soup)
   
    pretty_description = pretty_support + " / "  + pretty_extent

    return(pretty_description)

def get_location(soup):

    country = soup.country
    try:
        pretty_country = get_key(country)
        if not pretty_country:
            pretty_country = get_cleaned_text(country)    
    except:
        pretty_country = "Country unknown"

    settlement = soup.find('settlement')
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



# Get all data #

def get_list(links):
    mylist = []
    for url in links:
        soup = load_xml(url)

        #soup = load_xmls_by_id(id)
        #gets soup from url (without crawler)
        #soup = get_soup(url)

        tag = soup.msDesc
        handritID = str(tag['xml:id'])
        name = handritID[0:-3]
    
        location = get_location(soup)
        shorttitle = get_shorttitle(soup)
        creator = get_creator(soup)    
        description = get_description(soup)
        folio = get_folio(soup)
        origin = get_origin(soup)


        mytuple = (name,) + (creator,) + (shorttitle,) + (origin,) + (description,) + location + (folio,)

        structure = get_structure(mylist, mytuple) 

    return structure


# Citavify my data #

def summarize_location(location):
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

def citavify (links):
    mylist = []
    for url in links:
        soup = load_xml(url)
        #soup = load_xmls_by_id(id)

        tag = soup.msDesc
        handritID = str(tag['xml:id'])
        name = handritID[0:-3]
    
        creator = get_creator(soup)  
        shorttitle = get_shorttitle(soup)
        description = get_description(soup)
        date = "Eline?"
        origin = get_origin(soup)
        location = get_location(soup)
        settlement, archive, signature = summarize_location(location)
        
        mytuple = (name,) + (creator,) + (shorttitle,) + (description,) + (date,) + (origin,) + (settlement,) + (archive,) + (signature,)
        structure = get_structure(mylist, mytuple) 

    return structure

def citavi_panda (citavi_result):
    data = []
    data = pd.DataFrame(citavi_result)
    data.columns = ["Handrit-ID", "Creator", "Short title", "Description", "Dating", "Origin",  "Settlement", "Archive", "Signature"]
    file_name = "metadata_citavified"
    return data, file_name

def all_panda (result):
    data = []
    data = pd.DataFrame(result)
    data.columns = ["Handrit-ID", "Creator", "Short title", "Origin", "Description", "Country", "Settlement", "Institution", "Repository", "Collection", "Signature", "Folio"]
    file_name = "metadata"
    return data, file_name

# pandafy citavified data #
# citavi_result = citavify(myURLList)
# data, file_name = citavi_panda(citavi_result)

# pandafy all data #
result = get_list(myURLList) 
data, file_name = all_panda(result)

def CSVExport(FileName, DataFrame):
    DataFrame.to_csv(FileName+".csv", sep ='\t', encoding='utf-8', index=False)
    print("File exported")
    return

CSVExport(file_name, data)
print(data)