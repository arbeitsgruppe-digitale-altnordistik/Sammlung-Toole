from typing import List, Set
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd
from bs4 import BeautifulSoup
import util.metadata as metadata
import util.utils as utils
from util.constants import PREFIX_XML_DATA, PREFIX_XML_RAW
import zipfile
import glob
from lxml import etree


log = utils.get_logger(__name__)

# Data preparation
# ----------------

def has_data_available() -> bool:
    """Check if data is available"""
    xmls = glob.glob(PREFIX_XML_DATA + '*.xml')
    if xmls:
        log.info('XMLs found in directory.')
        return True
    log.info('No downloaded XMLs found.')
    return False


def unzipper() -> bool:
    """Unzips xml files from source directory into target directory. 
    Returns True on success.
    """
    zip = glob.glob(PREFIX_XML_RAW + '*.zip')
    if zip:
        with zipfile.ZipFile(zip[0], 'r') as file:
            file.extractall(PREFIX_XML_DATA)
            log.info('Extracted XMLs from zip file.')
            return True
    log.info('No zip file found. No data. Nothing to do.')
    return False


def _get_files_in_place() -> bool:
    """Will make sure there are XMLs in data folder, unzip if not, logs error if 
    there are no XMLs and no zip file. Returns false if no XMLs and no zip, true on
    success.
    """
    has_data = has_data_available()
    if not has_data:
        unzip = unzipper()
        if not unzip:
            log.error('Could not find any data!')
            return False
    return True
    

def load_xml_contents() -> pd.DataFrame:
    all_stored_xmls = glob.iglob(PREFIX_XML_DATA + '*xml')
    outDF = pd.DataFrame(columns=['shelfmark', 'content'])
    for individual_xml_file in all_stored_xmls:
        print(individual_xml_file)
        file_contents = _load_xml_file(individual_xml_file)
        shelfmark = _get_shelfmark(file_contents)
        outDF = outDF.append({'shelfmark': shelfmark, 'content': file_contents}, ignore_index=True)
    return outDF


def _load_xml_file(xml_file) -> str:
    with open(xml_file, encoding='utf-8', mode='r+') as file:
            return file.read()


def _get_shelfmark(content: str) -> str:
    try:
        root = etree.fromstring(content.encode())
        idno = root.find('.//msDesc/msIdentifier/idno', root.nsmap)
        log.debug(f'Shelfmark: {etree.tostring(idno)}')
        if idno is not None:
            return str(idno.text)
        else:
            return ""
    except Exception:
        log.exception(f"Faild to load Shelfmark XML:\n\n{content}\n\n")
        return ""


def deliver_handler_data() -> pd.DataFrame:
    """Will check if data is available and return a dataframe to the data handler.
    DataFrame has the following columns:
    'shelfmark': Shelfmark of the individual MS
    'content': The XML of the file as string
    """
    data_check = _get_files_in_place()
    if not data_check:
        return
    outDF = load_xml_contents()
    return outDF


# Data extraction
# ---------------

def get_msinfo(soup: BeautifulSoup):
    shorttitle = metadata.get_shorttitle(soup)
    signature, country, settlement, repository = metadata.get_msID(soup)
    origin = metadata.get_origin(soup)
    date, tp, ta, meandate, yearrange = metadata.get_date(soup)
    support = metadata.get_support(soup)
    folio = metadata.get_folio(soup)
    height, width = metadata.get_dimensions(soup)
    creator = metadata.get_creator(soup)
    extent = metadata.get_extent(soup)
    description = metadata.get_description(soup)

    return pd.Series({"signature": signature,
                      "shorttitle": shorttitle,
                      "country": country,
                      "settlement": settlement,
                      "repository": repository,
                      "origin": origin,
                      "date": date,
                      "tp": tp,
                      "ta": ta,
                      "meandate": meandate,
                      "yearrange": yearrange,
                      "support": support,
                      "folio": folio,
                      "height": height,
                      "width": width,
                      "extent": extent,
                      "description": description,
                      "creator": creator})