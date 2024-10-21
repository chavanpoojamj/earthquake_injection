import requests


def fetch_api_data(url):
    """
    This method will fetch data from API. using request Lib.
    :param url: API URL
    :return: json object
    """
    response = requests.get(url)
    return  response.json()