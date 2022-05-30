import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from bs4 import BeautifulSoup


# def get_soup(url: str, headers=None, params=None):
#     session = requests.Session()
#     retry = Retry(connect=10, backoff_factor=2)
#     adapter = HTTPAdapter(max_retries=retry)
#     session.mount('http://', adapter)
#     session.mount('https://', adapter)
#     try:
#         response = requests.get(url, headers=headers, params=params).text
#         soup = BeautifulSoup(response, 'lxml')
#         counter = 0
#         while soup is None and counter < 10:
#             response = session.get(url, headers=headers, params=params).text
#             soup = BeautifulSoup(response, 'lxml')
#             counter += 1
#         return soup
#     except Exception:
#         return None

def get_soup(url: str, headers=None, params=None):

    response = requests.get(url, headers=headers, params=params).text
    soup = BeautifulSoup(response, 'lxml')

    return soup
