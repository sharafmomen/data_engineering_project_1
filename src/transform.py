import pandas as pd 
import requests
from requests import Response
from bs4 import BeautifulSoup
import io
import re

def retrieve_year_quarter(col):
    match = re.match(r"(\d{4})\D+(\d)", col)
    if match:
        year, quarter = match.groups()
        return f"{year} {quarter}"
    return col

def melt_and_filter_rows(
        df: pd.DataFrame, 
        first_affect_quarter: tuple
):
    df.columns = [retrieve_year_quarter(col) for col in df.columns]
    print(df.columns)
    df_melted = pd.melt(df, id_vars=df.columns[0], var_name="year_quarter", value_name="supply")
    print(df_melted.head(10))


