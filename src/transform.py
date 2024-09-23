import pandas as pd 
import requests
from requests import Response
from bs4 import BeautifulSoup
import io
import os
import re
from datetime import datetime

def retrieve_year_quarter(col):
    match = re.match(r"(\d{4})\D+(\d)", col)
    if match:
        year, quarter = match.groups()
        return f"{year} {quarter}"
    return col

def remove_note_data(col_val):
    if "[" in col_val:
        idx = col_val.index("[")
        if idx > 0:
            idx = idx-1
            return col_val[:idx]
    return col_val

def filter_rows(
        value: str,
        filter_from: tuple, 
    ) -> bool:
    filter_from_str = str(filter_from[0]) + " " + str(filter_from[1])
    return value >= filter_from_str

def clean_df(
    df: pd.DataFrame
):
    clean_df = df.copy()

    # year quarter transformations
    if "year_quarter" in clean_df.columns:
        if ("year" not in clean_df.columns) and ("quarter" not in clean_df.columns):
            clean_df[["year", "quarter"]] = clean_df["year_quarter"].str.split(" ", expand=True).astype(int)
        clean_df = clean_df.drop("year_quarter", axis=1)

    # removing notes in square brackets in resource
    clean_df["resource"] = clean_df["resource"].apply(remove_note_data)

    # removing additional spaces in resource
    clean_df['resource'] = clean_df['resource'].str.strip()

    return clean_df

def melt_df(
    df: pd.DataFrame
):
    df.columns = [retrieve_year_quarter(col) for col in df.columns]
    df_melted = pd.melt(df, id_vars=df.columns[0], var_name="year_quarter", value_name="supply")
    df_melted = df_melted.rename(columns={df.columns[0]: "resource"})
    return df_melted

def add_dates(
    df: pd.DataFrame, 
    published_date: datetime
):
    df["date_published"] = published_date.date()
    df["date_processed"] = datetime.now()
    return df

def save_csv(df: pd.DataFrame, location: str = None):
    try:
        if location and os.path.isdir(location):  # check if location exists and is a directory
            df.to_csv(os.path.join(location, "DeltaTable.csv"), index=False)
            print(f"File saved in {location}")
            return  
    except Exception as e:
        print(f"Error saving to {location}: {e}, so stored in root directory of project")
    
    # if there's an error or location doesn't exis
    df.to_csv("DeltaTable.csv", index=False)
    
