from numpy import empty
import pandas as pd
import requests
from bs4 import BeautifulSoup


_coll_path = 'data/collections.csv'


def _get_soup(url: str, parser='xml') -> BeautifulSoup:
    """Get a BeautifulSoup object from a URL

    Args:
        url (str): The URL
        parser (str, optional): Parser; for HTML, use 'lxml'. Defaults to 'xml'.

    Returns:
        BeautifulSoup: BeautifulSoup object representation of the HTML/XML page.
    """
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, parser)
    return soup
    

def get_collections(use_cache=True, cache=True) -> pd.DataFrame:
    """Load all collections from handrit.isself.

    The dataframe contains the following informations:
    - Collection ID
    - Number of Manuscripts listed for the Collection
    - Collection URL

    Args:
        use_cache (bool, optional): Flag true if local cache should be used; false to force download. Defaults to True.
        cache (bool, optional): Flag true if result should be written to cache. Defaults to True.

    Returns:
        pd.DataFrame: Data frame containing basic information on collections.
    """
    if use_cache:
        cols = _load_collections_from_cache()
        if cols is not None and not cols.empty:
            return cols
    cols = _load_collections()
    if cache:
        cols.to_csv(_coll_path, encoding='utf-8', index=False)
    return cols


def _load_collections_from_cache():
    """Load collections from local cache
    """
    return pd.read_csv(_coll_path)


def _load_collections():
    """Load collections from website
    """
    soup = _get_soup('https://handrit.is/#collection', 'lxml')
    collection_tags = soup.find_all('div', attrs={'class': 'collection'})
    collections = [(c.find('span', attrs={'class': 'mark'}).text,
                    int(c.find('span', attrs={'class': 'count'}).text.split()[0]),
                    c.find('a', attrs={'class': 'viewall'})['href'].rsplit(';')[0] + '?showall.browser=1',) for c in collection_tags]
    df = pd.DataFrame(collections, columns=['collection', 'ms_count', 'url'])
    return df


if __name__ == "__main__":
    cols = get_collections()
    print(cols)
