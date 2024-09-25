import numpy as np
import pandas as pd 
import pandera as pa 
import requests
from requests import Response
from bs4 import BeautifulSoup
import io
import os
import re
from datetime import datetime

def retrieve_year_quarter(col: str) -> str:
    match = re.match(r"(\d{4})\D+(\d)", col)
    if match:
        year, quarter = match.groups()
        return f"{year} {quarter}"
    return col

def remove_note_data(col_val: str) -> str:
    """
    Some resources in the first column have notes. 

    It is usually after the name, and is in the format "resource [note x]"
    """
    return col_val.split("[")[0]

def retrieve_pie_category(first_col_value):
    """
    pie - production OR import OR export (OR other)
    """
    for kw in ["production", "import", "export"]:
        if kw in first_col_value:
            return kw
    return np.nan

def nullify_category_if_not_pie(row):
    """
    An additional step to ensure that the 'category' column values are made null
        when production, export, or import values are not appropriate.
    """
    for kw in ["production", 'import', 'export', "crude oil", "ngl", "feedstocks"]:
        if kw in row.iloc[0]:
            return row["category"]
    return np.nan

def extract_pie_df(
    df: pd.DataFrame
) -> pd.DataFrame:
    df_pie = df.copy()
    df_pie["category"] = df_pie[df_pie.columns[0]].apply(retrieve_pie_category)
    # reorder - to help with further cleaning efforts
    df_pie.insert(1, "category", df_pie.pop("category"))

    # assign categories
    df_pie["category"] = df_pie["category"].ffill() # will apply export to where it does not apply
    df_pie["category"] = df_pie.apply(nullify_category_if_not_pie, axis=1) # fix the above - 1
    df_pie["category"] = df_pie["category"].fillna("other") # fix the above - 2
    return df_pie

def melt_df(
    df: pd.DataFrame
) -> pd.DataFrame: 
    df.columns = [retrieve_year_quarter(col) for col in df.columns]
    df_melted = pd.melt(df, id_vars=df.columns[0:2], var_name="year_quarter", value_name="figures")
    df_melted = df_melted.rename(columns={df.columns[0]: "resource"})
    return df_melted

def clean_df(
    df: pd.DataFrame
) -> pd.DataFrame:
    clean_df = df.copy()

    # year quarter transformations
    clean_df[["year", "quarter"]] = clean_df["year_quarter"].str.split(" ", expand=True).astype(int)
    clean_df = clean_df.drop("year_quarter", axis=1)

    # removing notes in square brackets in resource
    clean_df["resource"] = clean_df["resource"].apply(remove_note_data)

    # removing additional spaces in resource
    clean_df['resource'] = clean_df['resource'].str.strip()

    return clean_df

def add_dates(
    df: pd.DataFrame, 
    published_date: datetime
) -> pd.DataFrame:
    dated_df = df.copy()
    dated_df["date_published"] = str(published_date.date())
    dated_df["date_processed"] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return dated_df

def add_filename(
    df: pd.DataFrame, 
    filename: str
) -> pd.DataFrame:
    f_df = df.copy()
    f_df["filename"] = filename
    return f_df

def save_csv(
    df: pd.DataFrame, 
    location: str = ""
) -> None:
    try:
        df.to_csv(f"{location}DeltaTable.csv", index=False)
    except Exception as e:
        df.to_csv("DeltaTable.csv", index=False)
        raise RuntimeError(f"Error saving to {location}: {e}, so stored in root directory of project")
    
