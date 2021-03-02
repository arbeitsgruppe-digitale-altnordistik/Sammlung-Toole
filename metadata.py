import requests
import lxml
from bs4 import BeautifulSoup
import urllib
import pandas as pd
import copy

#myURLList = "https://handrit.is/is/manuscript/xml/GKS04-2090-is.xml"
myURLList = ["https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/IB08-0165-is.xml", "https://handrit.is/is/manuscript/xml/Lbs02-0151-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
#myURLList = ["https://handrit.is/is/manuscript/xml/Lbs04-1495-is.xml", "https://handrit.is/is/manuscript/xml/Einkaeign-0021-is.xml"]



# Es fehlt noch Ursprungsort und Datierung.

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

def get_description(soup):
    support = soup.find('support')
    pretty_support = get_cleaned_text(support)

    extent = soup.find('extent')
    if extent:
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
        
        pretty_extent = get_cleaned_text(extent)
    else:
        pretty_extent = ""         

    pretty_description = pretty_support + " / " + pretty_extent + " / " + pretty_dimensions

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
print(data)





