import pandas as pd 
import pandera as pa 
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

def melt_df(
    df: pd.DataFrame
):
    df.columns = [retrieve_year_quarter(col) for col in df.columns]
    df_melted = pd.melt(df, id_vars=df.columns[0], var_name="year_quarter", value_name="supply")
    df_melted = df_melted.rename(columns={df.columns[0]: "resource"})
    return df_melted

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

def add_dates(
    df: pd.DataFrame, 
    published_date: datetime
):
    dated_df = df.copy()
    dated_df["date_published"] = str(published_date.date())
    dated_df["date_processed"] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return dated_df

def add_filename(
    df: pd.DataFrame, 
    filename: str
):
    f_df = df.copy()
    f_df["filename"] = filename
    return f_df

def output_schema_validation(
    df = pd.DataFrame
):
    # order of columns, because pandera doesn't support
    column_order = ["resource", "supply", "year", "quarter", "date_published", "date_processed", "filename"]
    assert list(df.columns) == column_order, "not in correct order or unexpected columns"

    # confirm schema
    schema = pa.DataFrameSchema({
        "resource": pa.Column(pa.String, nullable=False), 
        "supply": pa.Column(pa.Float, nullable=True), # Although I saw 0s in there, instead of nulls
        "year": pa.Column(pa.Int, nullable=False), 
        "quarter": pa.Column(pa.Int, nullable=False), 
        "date_published": pa.Column(pa.String, pa.Check.str_matches(r"\d{4}-\d{2}-\d{2}"), nullable=False), 
        "date_processed": pa.Column(pa.DateTime, nullable=False), 
        "filename": pa.Column(pa.String, nullable=False)
    })

    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        error_columns = e.failure_cases['column'].unique()
        raise ValueError(f"Errors seen in {error_columns}")
    
    # confirm correct date format
    assert df['date_processed'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') == str(x)).all(), "date_processed incorrect time representation"

def save_csv(
    df: pd.DataFrame, 
    location: str = ""
):
    try:
        df.to_csv(f"{location}DeltaTable.csv", index=False)
    except Exception as e:
        df.to_csv("DeltaTable.csv", index=False)
        raise RuntimeError(f"Error saving to {location}: {e}, so stored in root directory of project")
    
