import pandas as pd 
import requests
from requests import Response
from bs4 import BeautifulSoup
import io
import re
import dateutil.parser

def extract_from_top_link() -> Response:
    link = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
    response = requests.get(link)
    return response



def get_excel_link(response: Response) -> str:
    response = extract_from_top_link()
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a", class_="govuk-link gem-c-attachment__link")
        for link in links:
            if "Supply and use of crude oil, natural gas liquids and feedstocks" in link.text:
                excel_link = link["href"]
                return excel_link

def get_info(excel_link):
    response = requests.get(excel_link)

    # getting the dfs
    stored_excel_file = pd.ExcelFile(io.BytesIO(response.content))
    df_published_info = pd.read_excel(stored_excel_file, sheet_name="Cover Sheet", skiprows=3, header=None)
    df_quarter = pd.read_excel(stored_excel_file, sheet_name="Quarter", skiprows=4)

    # published date
    published_date_str = " ".join(df_published_info.iloc[0,0].split("\n")[0].split(" ")[-3:])
    published_date_ts = dateutil.parser.parse(published_date_str)

    return published_date_ts, df_quarter, 

