import requests
import lxml
from bs4 import BeautifulSoup
import urllib
import pandas as pd
import copy
from crawler import load_xml
from crawler import load_xmls_by_id



#myURLList = ["https://handrit.is/is/manuscript/xml/AM02-0197-en.xml", "https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml", "https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/IB08-0165-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0110-I-II-is.xml", "https://handrit.is/is/manuscript/xml/AM08-0048-is.xml"]

#myURLList = ["https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml"]

# Hier Probleme noch:
myURLList = ["https://handrit.is/is/manuscript/xml/Lbs02-0152-is.xml"]


# Es fehlt noch Ursprungsort und Datierung.

# get_soup könnte getilgt werden, da dies mit dem Crawler nun bewerkstelligt wird.
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

def get_creator(soup):
    pretty_creators = ""
    hands = soup.handDesc
    creators = hands.find_all('name', {'type':'person'}) 
    if not creators:
        pretty_creators = "Scribe unknown"
    
    else:
        for creator in creators:
            pretty_creator = get_cleaned_text(creator)
            pretty_creators = pretty_creators + "; " + pretty_creator
        pretty_creators = pretty_creators[2:]    
    
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
    else:
        support = soup.find("country")
        try:
            pretty_support = get_cleaned_text(support)
        except:
            pretty_support = ""
    
    return(pretty_support)

# hier in get_extent zieht er nur string zwischen tags, aber keine info aus tag selbst, gilt auch für unit
def get_extent(soup):
    extent = soup.find('extent')

    extent_copy = copy.copy(extent)

    dimensions = extent_copy.find('dimensions')

    while dimensions:
        height = dimensions.height
        width = dimensions.width

        height.string = height.string + " x"
        width.string = width.string + " mm"
        
        extent_copy.dimensions.unwrap()
        dimensions = extent_copy.find('dimensions')

    pretty_extent = get_cleaned_text(extent_copy)

    if not pretty_extent:
        pretty_extent = ""
    
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

    print(pretty_description)

    return(pretty_description)

def get_location(soup):
    country = soup.find('country', {'key':'IS'})
    if country:
        country = country.get('key')
        if country == "IS":
            pretty_country = "Iceland"
        elif country == "DK":
            pretty_country = "Denmark"
        elif country == "FO":
            pretty_country = "Faroe Islands"
        else:
            pretty_country = country
    else:
        country = soup.find("country")
        pretty_country = get_cleaned_text(country)
   
## Für Citavi müsste hier für Archiv-Eingabefläche zusammen gefasst werden (Achtung. Abhängig von Institution)
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

def get_list(links):
    mylist = []
    for url in links:
        #soup = load_xml(url)
        #soup = load_xmls_by_id(id)

        #gets soup from url (without crawler)
        soup = get_soup(url)

        tag = soup.msDesc
        handritID = str(tag['xml:id'])
        name = handritID[0:-3]
    
        location = get_location(soup)
        shorttitle = get_shorttitle(soup)
        creator = get_creator(soup)    
        description = get_description(soup)

        mytuple = (name,) + (creator,) + (shorttitle,) + (description,) + location

        structure = get_structure(mylist, mytuple) 

    return structure


def get_structure(mylist, mytuple):
    mylist.append(mytuple)
    return mylist

result = get_list(myURLList) 
data = []
data = pd.DataFrame(result)
data.columns = ["ID", "Handrit-ID", "Creator", "Short title", "Description", "Country", "Settlement", "Repository", "Collection", "Signature"]
 
def CSVExport(FileName, DataFrame):
    DataFrame.to_csv(FileName+".csv", encoding='utf-8')
    print("File exported")
    return

#CSVExport("Meta", data)
#print(data)





