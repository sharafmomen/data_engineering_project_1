import pandas as pd 
import requests
from requests import Response
from bs4 import BeautifulSoup
import io
import re

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
    stored_excel_file = pd.ExcelFile(io.BytesIO(response.content))
    df_affected_period = pd.read_excel(stored_excel_file, sheet_name="Cover Sheet", skiprows=7, header=None)
    affected_period_str = df_affected_period.iloc[0,0]
    quarters_mentioned = re.findall(r"Quarter (\d) (\d{4})", affected_period_str)
    year_quarter_lst = [(int(y), int(q)) for q, y in quarters_mentioned]
    lowest = min(year_quarter_lst, key=lambda x: (x[0], x[1]))
    df_quarter = pd.read_excel(stored_excel_file, sheet_name="Quarter", skiprows=4)
    return lowest, df_quarter

