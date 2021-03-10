from bs4 import BeautifulSoup
import pandas as pd
import csv
from pathlib import Path
import lxml
import html5lib


_xml_path = Path('data/xml/')

resDict = {'XMLID': [], 'Shelfmark': [], 'Language': [], 'Terminus Postquem': [], 'Terminus Antequem': []}
for files in _xml_path.glob(('*.xml')):
    try:
        with open(files, encoding='UTF-8') as xml:
            soup = BeautifulSoup(xml, 'xml')
        _id = soup.msDesc['xml:id']
        _shelf = soup.msDesc.idno.get_text()
        _lang = soup.msDesc['xml:lang']
        try:
            postquem = soup.msDesc.origDate['notBefore']
            antequem = soup.msDesc.origDate['notAfter']
        except:
            postquem = "N/A"
            antequem = "N/A
    except:
        try:
            # import pdb; pdb.set_trace()
            with open(files, 'rb') as xml:
                soup = BeautifulSoup(xml, features='xml', from_encoding='iso-8859-1')
            _id = soup.msDesc['xml:id']
            _shelf = soup.msDesc.idno.get_text()
            _lang = soup.msDesc['xml:lang']
        except:
            print("Gods dammit, handrit!")
    try:
        postquem = soup.msDesc.origDate['notBefore']
        antequem = soup.msDesc.origDate['notAfter']
    except:
        postquem = "N/A"
        antequem = "N/A"
    
    resDict['XMLID'].append(_id)
    resDict['Shelfmark'].append(_shelf)
    resDict['Language'].append(_lang)
    resDict['Terminus Postquem'].append(postquem)
    resDict['Terminus Antequem'].append(antequem)

datingPD = pd.DataFrame.from_dict(resDict)
print(datingPD)