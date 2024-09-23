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

def extract_from_link(link, retries) -> Response:
    for r in range(retries):
        response = requests.get(link)
        if response.status_code == 200:
            return response
        logging.error(f"Issue with get request from link - {response.status_code}")
        time.sleep(10)
    
    raise RuntimeError(f"Unsuccessful get request from link after {retries} attempts. Last status code: {response.status_code}")

def get_excel_link(response: Response) -> str:
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all("a", class_="govuk-link gem-c-attachment__link")
    for link in links:
        if "Supply and use of crude oil, natural gas liquids and feedstocks" in link.text:
            excel_link = link["href"]
            return excel_link
    raise RuntimeError(f"File link not found - Supply and use of crude oil, natural gas liquids and feedstocks")

def retrieve_filename(excel_link: str) -> str:
    return excel_link.split("/")[-1]

def confirm_new_file( 
    current_filename: str, 
    download_if_not_new: bool = False, 
    csv_location: str = "",
) -> bool:
    """
    Note: 

    args:
        - download_if_not_new suggests that, if a file is detected and it is NOT new 
            in reference to the existing DeltaTable.csv, it will still download (to see how it works)
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
    try:
        df_published_info = pd.read_excel(excel_file, sheet_name="Cover Sheet", skiprows=3, header=None)
        published_date_str = " ".join(df_published_info.iloc[0,0].split("\n")[0].split(" ")[-3:])
        published_date_ts = dateutil.parser.parse(published_date_str)
        return published_date_ts
    except Exception as e:
        raise RuntimeError(f"Issue with retrieving published date: {e}")

def extract_resource_df(excel_file: pd.ExcelFile) -> pd.DataFrame:
    df_quarter = pd.read_excel(excel_file, sheet_name="Quarter", skiprows=4)
    return df_quarter
            
def get_info(response: Response) -> tuple[datetime, pd.DataFrame]:
    stored_excel_file = pd.ExcelFile(io.BytesIO(response.content))
    df_quarter = extract_resource_df(stored_excel_file)
    published_date_ts = extract_published_date(stored_excel_file)
    return published_date_ts, df_quarter

def input_schema_validation(df: pd.DataFrame) -> None:
    assert df[df.columns[0]].dtype in ("object", str), "input excel type incorrect - should be string"
    for col_idx in range(1,len(df.columns)):
        assert df[df.columns[col_idx]].dtype in (np.int64, np.float64), "input excel type incorrect - should be numeric"

