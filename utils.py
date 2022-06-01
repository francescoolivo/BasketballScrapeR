import requests
from bs4 import BeautifulSoup


def get_soup(url: str, headers=None, params=None):
    response = requests.get(url, headers=headers, params=params).text
    soup = BeautifulSoup(response, 'lxml')

    return soup
