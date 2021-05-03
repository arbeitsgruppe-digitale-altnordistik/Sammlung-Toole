import logging
import requests
from bs4 import BeautifulSoup


def get_soup(url: str, parser='xml') -> BeautifulSoup:
    """Get a BeautifulSoup object from a URL
    Args:
        url (str): The URL
        parser (str, optional): Parser; for HTML, use 'lxml'. Defaults to 'xml'.
    Returns:
        BeautifulSoup: BeautifulSoup object representation of the HTML/XML page.
    """
    log.debug(f'Requesting ({parser}): {url}')
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, parser)
    return soup


def get_logger(name: str) -> logging.Logger:
    """
    returns a preconfigured logger
    """
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('warnings.log', mode='a')
    f_handler2 = logging.FileHandler('log.log', mode='w')
    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.WARNING)
    f_handler2.setLevel(logging.DEBUG)
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(format)
    f_handler.setFormatter(format)
    f_handler2.setFormatter(format)
    log.addHandler(c_handler)
    log.addHandler(f_handler)
    log.addHandler(f_handler2)
    return log


log = get_logger(__name__)