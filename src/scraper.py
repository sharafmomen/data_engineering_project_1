import numpy as np
import pandas as pd 
import requests
import time
from requests import Response
from bs4 import BeautifulSoup
import io
import re
import os
from datetime import datetime
import dateutil.parser
import logging

def extract_from_link(link: str, retries: int, delay: int = 10) -> Response:
    """
    Gets content from actual government site.
    Attempts a number of retries. Each time it does not work, an error is logged with, along with status code. 

    If NOTHING is retrieved, the rest of the code is useless, so a RuntimeError is thrown to stop the code. 
    """

    for r in range(retries):
        response = requests.get(link)
        if response.status_code == 200:
            return response
        logging.error(f"Issue with get request from link - {response.status_code}")
        time.sleep(delay)
    
    raise RuntimeError(f"Unsuccessful get request from link after {retries} attempts. Last status code: {response.status_code}")

def get_excel_link(response: Response) -> str:
    """
    Loops through all elements in response content, where the class attribute is:
        'govuk-link gem-c-attachment__link'

    Going through the list, if one of them has the specific text, then it is selected

    If nothing is found, the rest of the code in main.py is futile, so throws an error. 
    """
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all("a", class_="govuk-link gem-c-attachment__link")
    for link in links:
        if "Supply and use of crude oil, natural gas liquids and feedstocks" in link.text:
            excel_link = link["href"]
            return excel_link
    raise RuntimeError(f"File link not found - Supply and use of crude oil, natural gas liquids and feedstocks")

def retrieve_filename(excel_link: str) -> str:
    """
    Filename can be retrieved from the excel link. Using '/' as a delimiter, getting the last element of a list
        will surely have the filename and end in .xlsx
    """
    return excel_link.split("/")[-1]

def confirm_new_file( 
    current_filename: str, 
    download_if_not_new: bool = False, 
    csv_location: str = "",
) -> bool:
    """
    Checks to see if there's a new file or not. If there is, then we can carry on through 
        with the data pipeline process. 


    - current_filename is the filename extracted from the excel link. 
    - download_if_not_new suggests that, if a file is detected and it is NOT new 
        in reference to the existing DeltaTable.csv, it will still download (to see how it works)
    - csv_location is the folder from the root directory which hosts the csv file from
         which we can read the previous filename from
    """

    if download_if_not_new:
        return True

    csv_path = f"{csv_location}DeltaTable.csv"

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path) # in reality, this process would have a scalar query from a table on the cloud
        unique_values = df["filename"].unique().tolist()
        if current_filename in unique_values:
            return False
    return True

def extract_published_date(excel_file: pd.ExcelFile) -> datetime:
    """
    Extracts the date using a set of hardcoded rules from the first tab of the excel file. 

    Use of dateutil.parser, as it is applicable to a range of date formats. 
    """
    try:
        df_published_info = pd.read_excel(excel_file, sheet_name="Cover Sheet", skiprows=3, header=None)
        published_date_str = " ".join(df_published_info.iloc[0,0].split("\n")[0].split(" ")[-3:])
        published_date_ts = dateutil.parser.parse(published_date_str)
        return published_date_ts
    except Exception as e:
        raise RuntimeError(f"Issue with retrieving published date: {e}")

def extract_resource_df(excel_file: pd.ExcelFile) -> pd.DataFrame:
    """
    Extracts the main df from the Quarter tab with the resource production, import, export, and other info. 
    """
    df_quarter = pd.read_excel(excel_file, sheet_name="Quarter", skiprows=4)
    df_quarter[df_quarter.columns[0]] = df_quarter[df_quarter.columns[0]].str.lower() # lowers so it's easy to debug
    return df_quarter
            
def get_info(response: Response = None) -> tuple[datetime, pd.DataFrame]:
    """
    Produces a published date timestamp and the dataframe from Quarter worksheet
    """

    stored_excel_file = pd.ExcelFile(io.BytesIO(response.content))
    df_quarter = extract_resource_df(stored_excel_file)
    published_date_ts = extract_published_date(stored_excel_file)
    return published_date_ts, df_quarter

