from typing import Set
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd
import statistics
import crawler
import requests
from bs4 import BeautifulSoup

# MS Texts
# --------


def get_mstext(soup: BeautifulSoup) -> Set:
    # TODO: documentation
    texts = set()
    ms_itmes = soup.find_all("msItem")
    for item in ms_itmes:
        title: Tag = item.title
        texts.add(title.get_text())
        # TODO: beautify string?
        # TODO: multiple titles? no title? rubric?
    return texts


def get_mstexts(soups):
    # for soup in soups:
    #     pass  # TODO
    return pd.DataFrame()


# MS Infos
# --------


def get_msinfo_(soup: BeautifulSoup):  # TODO: get rid of this, once it works with series
    msID = soup.find("msIdentifier")
    if not msID:
        country, settlement, repository, signature = "", "", "", ""
    else:
        country = msID.find("country").get_text()
        country = msID.find("settlement").get_text()
        country = msID.find("repository").get_text()
        country = msID.find("idno").get_text()

    date, tp, ta, meandate, yearrange = get_date(soup)

    return country, settlement, repository, signature, date, tp, ta, meandate, yearrange


def get_msinfo(soup: BeautifulSoup):
    msID = soup.find("msIdentifier")
    if not msID:
        country, settlement, repository, signature = "", "", "", ""
    else:
        country = msID.find("country").get_text()
        settlement = msID.find("settlement").get_text()
        repository = msID.find("repository").get_text()
        signature = msID.find("idno").get_text()

    date, tp, ta, meandate, yearrange = get_date(soup)
    
    return pd.Series({"country": country,
                      "settlement": settlement,
                      "repository": repository,
                      "signature": signature,
                      "date": date,
                      "tp": tp,
                      "ta": ta,
                      "meandate": meandate,
                      "yearrange": yearrange})


def get_date(soup: BeautifulSoup):
    tag = soup.origDate
    date = ""
    ta = 0
    tp = 0
    meandate = 0
    yearrange = 0
    if not tag:
        return date, tp, ta, meandate, yearrange  # TODO: error handling

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
        tp = int(date)
        ta = int(date)
        meandate = tp
        yearrange = 0

    elif tag.get("from") and tag.get("to"):
        fr = str(tag["from"])
        to = str(tag["to"])
        date = f"{fr}-{to}"
        tp = int(fr)
        ta = int(to)
        meandate = int(statistics.mean([int(tp), int(ta)]))
        yearrange = int(ta) - int(tp)

    return date, tp, ta, meandate, yearrange


def get_msinfos(soups):
    data = pd.DataFrame(columns=["Handrit ID",
                                 "Signature",
                                 "Country",
                                 "Settlement",
                                 "Repository",
                                 "Original Date",
                                 "Mean Date",
                                 "Range",
                                 ])

    for soup in soups:
        tag = soup.msDesc
        try:
            handritID = str(tag["xml:id"])
            handritID = handritID[0:-3]
        except:
            handritID = "N/A"

        country, settlement, repository, signature, date, tp, ta, meandate, yearrange = get_msinfo_(soup)

        data = data.append({"Handrit ID": handritID,
                            "Signature": signature,
                            "Country": country,
                            "Settlement": settlement,
                            "Repository": repository,
                            "Original Date": date,
                            "Terminus Postquem": tp,
                            "Terminus Antequem": ta,
                            "Mean Date": meandate,
                            "Range": yearrange,
                            },
                           ignore_index=True,
                           )

    return data


# TODO: move the following to crawler, remove possible duplication
# ------------------------------------


def get_search_result_pages(url):
    """Get multiple result pages from search with 26+ hits.

    This function returns a list of all result pages from one search result,
    if the search got too many hits to display on one page.

    Args:
        url (str): URL to a multi page search result

    Returns:
        List[str]: a list with URLs for all pages of the search result
    """
    res = [url]
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, "lxml")
    links = soup.select("div.t-data-grid-pager > a")
    urls = [l["href"] for l in links]
    for u in urls:
        if u not in res:
            res.append(u)
    return res


def get_shelfmarks_from_urls(urls):
    results = []
    if len(urls) == 1:
        url = urls[0]
        results += get_shelfmarks(url)
        return list(set(results))
    for url in urls:
        results += get_shelfmarks(url)
    return list(set(results))


def get_id_from_shelfmark_local(shelfmarks: list) -> list:
    _shelfmark_path = "data/ms_shelfmarks.csv"
    shelfIDPD = pd.read_csv(_shelfmark_path)
    shelfIDPD = shelfIDPD[shelfIDPD["shelfmark"].isin(shelfmarks)]
    idList = shelfIDPD["id"].tolist()
    return idList


def get_shelfmarks(url):
    """Get Shelfmarks from an URL

    This function returns a list of strings containing shelfmarks from a page on handrit.is.

    Args:
        url (str): a URL to a search result page on handrit.is

    Returns:
        List[str]: A list of Shelfmarks
    """
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, "lxml")
    subsoups = soup.select("td.shelfmark")
    shelfmarks = [ss.get_text() for ss in subsoups]
    shelfmarks = [sm.strip() for sm in shelfmarks]
    return shelfmarks


def efnisordResult(inURL):
    resultPage = requests.get(inURL).content
    pho = BeautifulSoup(resultPage, "lxml")
    theGoods = pho.find("tbody")
    identifierSoup = theGoods.find_all(class_="id")
    identifierList = []
    for indi in identifierSoup:
        identifier = indi.get_text()
        identifierList.append(identifier)
    return identifierList


def get_data_from_browse_url(url: str, DataType: str):
    """Get the desired data from a handrit browse URL.
      The data frame to be returned depends on the DataType variable (cf. below).
      If DataType = Contents:
          Data frame columns will be the shelfmarks/IDs of the MSs, each column containing the text
          witnesses listed in the MS description/XML.

      If DataType = Metadata:
          Data frame contains the following columns:
          ['Handrit ID', 'Signature', 'Country',
                                  'Settlement', 'Repository', 'Original Date', 'Mean Date', 'Range']

    Args:
        inURL(str, required): A URL pointing to a handrit browse result page.
        DataType(str, required): Whether you want to extract the contents of MSs from the XMLs or metadata
        such as datings and repository etc. (cf. above). Can be 'Contents' or 'Metadata'

    Returns:
        pd.DataFrame: DataFrame containing MS contents or meta data.
    """
    ids = efnisordResult(url)
    print(f"Got {len(ids)} IDs.")
    xmls = []
    for i in ids:
        xml = crawler.load_xmls_by_id(i)
        xmlList = list(xml.values())
        for x in xmlList:
            xmls.append(x)
    if DataType == "Contents":
        data = get_mstexts(xmls)
    if DataType == "Metadata":
        data = get_msinfos(xmls)
    return data


def get_data_from_search_url(url: str, DataType: str):
    """This will get the requested data from the corresponding XML files and return it as a data frame.

    The data frame to be returned depends on the DataType variable (cf. below).
      If DataType = Contents:
          Data frame columns will be the shelfmarks/IDs of the MSs, each column containing the text
          witnesses listed in the MS description/XML.

      If DataType = Metadata:
          Data frame contains the following columns:
          ['Handrit ID', 'Signature', 'Country',
                                 'Settlement', 'Repository', 'Original Date', 'Mean Date', 'Range']

    Args:
        inURL(str, required): A URL pointing to a handrit search result page.
        DataType(str, required): Whether you want to extract the contents of MSs from the XMLs or metadata
        such as datings and repository etc. (cf. above). Can be 'Contents' or 'Metadata'

    Returns:
        pd.DataFrame: DataFrame containing MS contents or meta data.
    """
    pages = get_search_result_pages(url)
    print(f"Got {len(pages)} pages.")
    shelfmarks = get_shelfmarks_from_urls(pages)
    print(f"Got {len(shelfmarks)} shelfmarks.")
    ids = get_id_from_shelfmark_local(shelfmarks)
    xmls = []
    for i in ids:
        xml = crawler.load_xmls_by_id(i)
        xmlList = list(xml.values())
        for x in xmlList:
            xmls.append(x)
    print(f"Got {len(xmls)} XML files")
    if DataType == "Contents":
        data = get_mstexts(xmls)
    if DataType == "Metadata":
        data = get_msinfos(xmls)
    return data


def get_from_search_list(inURLs: list, DataType: str, joinMode: str):
    """This will get the requested data from the corresponding XML files and return it as a data frame.

    The data frame to be returned depends on the DataType variable (cf. below).
      If DataType = Contents:
          Data frame columns will be the shelfmarks/IDs of the MSs, each column containing the text
          witnesses listed in the MS description/XML.

      If DataType = Metadata:
          Data frame contains the following columns:
          ['Handrit ID', 'Signature', 'Country',
                                 'Settlement', 'Repository', 'Original Date', 'Mean Date', 'Range']

    Args:
        inURLs(list, required): A URL pointing to a handrit search result page.
        DataType(str, required): Whether you want to extract the contents of MSs from the XMLs or metadata
          such as datings and repository etc. (cf. above). Can be 'Contents' or 'Metadata'
        joinMode(str, required): Whether you want all info or only those that occur in the results of all
          search URLs passed as input. If 'shared' returns empty, it means there is no overlap.
          Set 'All' if you want to return all MSs and their data (duplicates will be removed).
          Set 'Shared' if you only want the MSs occuring in all search result URLs.

    Returns:
        pd.DataFrame: DataFrame containing MS contents or meta data.
    """
    listList = []
    for url in inURLs:
        pages = get_search_result_pages(url)
        print(f"Got {len(pages)} pages.")
        shelfmarks = get_shelfmarks_from_urls(pages)
        listList.append(shelfmarks)
    if joinMode == "Shared":
        finalMSs = list(set.intersection(*map(set, listList)))
    if joinMode == "All":
        allTheStuff = [i for x in listList for i in x]
        finalMSs = list(set(allTheStuff))
    ids = get_id_from_shelfmark_local(finalMSs)
    xmls = []
    for i in ids:
        xml = crawler.load_xmls_by_id(i)
        xmlList = list(xml.values())
        for x in xmlList:
            xmls.append(x)
    print(f"Got {len(xmls)} XML files")
    if DataType == "Contents":
        data = get_mstexts(xmls)
    if DataType == "Metadata":
        data = get_msinfos(xmls)
    return data
