import pandas as pd 
import requests
from requests import Response
from bs4 import BeautifulSoup

def extract_from_top_link() -> Response:
    link = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
    response = requests.get(link)
    return response

def get_tables():
    response = extract_from_top_link()
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a", class_="govuk-link gem-c-attachment__link")
        counter = 0
        for link in links:
            counter += 1
            print(counter)
            if "Supply and use of crude oil, natural gas liquids and feedstocks" in link.text:
                excel_link = link["href"]
                print("excel_link")
                break

get_tables()

