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

def melt_df(
        df: pd.DataFrame
):
    df.columns = [retrieve_year_quarter(col) for col in df.columns]
    df_melted = pd.melt(df, id_vars=df.columns[0], var_name="year_quarter", value_name="supply")
    df_melted = df_melted.rename(columns={df.columns[0]: "resource"})
    df_melted[["year", "quarter"]] = df_melted["year_quarter"].str.split(" ", expand=True).astype(int)
    df_melted["resource"] = df_melted["resource"].apply(remove_note_data)
    return df_melted

def filter_df(
    df: pd.DataFrame, 
    first_affect_quarter: tuple
): 
    """
    filters and also removes column NO LONGER necessary: year_quarter
    """
    filter_condition = df["year_quarter"].apply(filter_rows, args=(first_affect_quarter,))
    df_filtered = df[filter_condition]
    df_filtered = df_filtered.drop("year_quarter", axis=1)
    return df_filtered


