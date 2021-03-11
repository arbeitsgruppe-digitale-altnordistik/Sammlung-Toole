from bs4 import BeautifulSoup
import pandas as pd
import csv
from pathlib import Path
import lxml
import html5lib
import plotly.express as px
import os


_xml_path = Path('data/xml/')
_date_path = 'data/dating_all.csv'
_html_path = 'data/dating_all.html'

def all_date_data(use_cache: bool = True):
    if use_cache and os.path.exists(_date_path):
        datingPD = pd.read_csv(_date_path)
        return datingPD
    resDict = {'XMLID': [], 'Shelfmark': [], 'Language': [], 'Terminus Postquem': [], 'Terminus Antequem': []}
    for files in _xml_path.glob(('*.xml')):
        with open(files, encoding='UTF-8') as xml:
            soup = BeautifulSoup(xml, 'xml')
        _id = soup.msDesc['xml:id']
        _shelf = soup.msDesc.idno.get_text()
        _lang = soup.msDesc['xml:lang']
        try:
            postquem = soup.msDesc.origDate['notBefore']
            antequem = soup.msDesc.origDate['notAfter']

            if len(postquem) != 4:
                year, month, day = postquem.split('-')
                postquem = year
            
            if len(antequem) != 4:
                year, month, day = postquem.split('-')
                antequem = year

            resDict['XMLID'].append(_id)
            resDict['Shelfmark'].append(_shelf)
            resDict['Language'].append(_lang)
            resDict['Terminus Postquem'].append(postquem)
            resDict['Terminus Antequem'].append(antequem)
            

        except:
            postquem = "N/A"
            antequem = "N/A"
        
        

    datingPD = pd.DataFrame.from_dict(resDict).dropna()
    datingPD.to_csv(_date_path, encoding='utf-8', index=False)
    return datingPD

def do_plot(use_cache: bool = True):
    datePD = all_date_data(use_cache=use_cache)
    inDF = datePD.drop_duplicates(subset='Shelfmark')
    print("Doing scatter plot")
    fig = px.scatter(inDF, x='Terminus Postquem', y='Terminus Antequem', color='Shelfmark')
    fig.write_html(_html_path)
    print("Done") 

if __name__ == "__main__":
    do_plot()
