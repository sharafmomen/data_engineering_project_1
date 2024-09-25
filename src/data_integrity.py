import numpy as np
import pandas as pd 
import pandera as pa 
from src.transform import retrieve_year_quarter

def input_rows_check(df: pd.DataFrame) -> None:
    """
    checks if number of rows is >= 10. 
    
    The reason is because it includes total production, exports, imports 
        and the important materials that fall under them. 

    Also checks if production, import, export are provided, and if crude oil, ngls and feedstock is repeated
        at least 3 times, regardless of whether they're grouped (crude oil and ngls) or separate. 

    The above allows for flexibility for when ngls and crude oil data have their own figures. At the moment, 
        it's only like that under Indigenous Production. 
    """

    # minimum row count
    assert len(df) >= 10, "input excel - too little rows, please investigate"

    # expected values and value counts 
    val_cnts_min = {
        "production": 1, 
        "import": 1, 
        "export": 1, 
        "crude oil": 3, 
        "ngls": 3, 
        "feedstock": 3
    }

    val_cnts = {}

    for k in val_cnts_min:
        val_cnts[k] = (df[df.columns[0]].str.contains(k)).sum()

    assert all(val_cnts[key] >= val_cnts_min[key] for key in val_cnts_min), "input excel - not enough repetitions of materials"

def input_duplicate_rows(df: pd.DataFrame) -> None:
    """
    - Checks if there are any duplicate rows
    """
    assert not df.duplicated().any(), "input excel - resources repeated"

def input_nulls_check(df: pd.DataFrame) -> None:
    """
    - checks if null % is less than 10%
    - ensures there are no nulls in column 1
    """
    assert not df[df.columns[0]].isnull().any(), "input excel - unexpected nulls in first column, resources"
    assert df.iloc[:, 1:].isnull().mean().mean() <= 0.1, "input excel - nulls make up more than 10% of dataset, please investigate"

def input_temporal_integrity(df: pd.DataFrame) -> None:
    """
    checks:
        - for if there are maximum 4 repeated years (meaning 4 quarters)
        - for if the order of those years and quarters is correct
    """

    # order
    year_quarter_list = [retrieve_year_quarter(col) for col in df.columns[1:]]
    assert year_quarter_list == sorted(year_quarter_list), "input excel - unexpected ordering of columns"
    
    # checking if there are at most 4 quarters for a given year
    year_quarter_list2 = [[year_quarter[:4], year_quarter[-1]] for year_quarter in year_quarter_list]
    year_quarter_df = pd.DataFrame(year_quarter_list2, columns=["year", "quarter"])
    year_cnt = year_quarter_df["year"].value_counts()
    assert (year_cnt <= 4).all(), "input excel - too many quarters for a year"

    # checking if a period was repeated twice
    assert ~year_quarter_df.duplicated(subset=["year", "quarter"]).any(), "input excel - repeated year and quarter"
    

def input_allowable_negative_quantities(df: pd.DataFrame) -> None:
    """
    Checks if there are any negative values for resources that don't expect a negative value
    """
    starts_with = (
        "stock change", 
        "transfers", 
        "statistical difference"
    )

    condition_df = df[~df[df.columns[0]].str.lower().str.startswith(starts_with)]

    assert (condition_df[df.columns[1:]] >= 0).all().all(), "input excel - negative values for unexpected resources"

def input_checks(df: pd.DataFrame) -> None:
    """
    Does all the input integrity checks, aside from schema check
    """
    input_rows_check(df)
    input_nulls_check(df)
    input_duplicate_rows(df)
    input_temporal_integrity(df)
    input_allowable_negative_quantities(df)

def input_schema_validation(df: pd.DataFrame) -> None:

    # First column - doesn't generally have a name, so the column is automatically titled something
    assert df[df.columns[0]].dtype in ("object", str), "input excel type incorrect - should be string"

    # Iterates through the other columns
    for col_idx in range(1,len(df.columns)):
        assert df[df.columns[col_idx]].dtype in (np.int64, np.float64), "input excel type incorrect - should be numeric"

def output_schema_validation(
    df = pd.DataFrame
) -> None:
    # order of columns, because pandera doesn't support
    column_order = ["resource", "category", "figures", "year", "quarter", "date_published", "date_processed", "filename"]
    assert list(df.columns) == column_order, "not in correct order or unexpected columns"

    # confirm schema
    schema = pa.DataFrameSchema({
        "resource": pa.Column(pa.String, nullable=False), 
        "category": pa.Column(pa.String, nullable=False), 
        "figures": pa.Column(pa.Float, nullable=True), # Although I saw 0s in there, instead of nulls
        "year": pa.Column(pa.Int, nullable=False), 
        "quarter": pa.Column(pa.Int, nullable=False), 
        "date_published": pa.Column(pa.String, pa.Check.str_matches(r"\d{4}-\d{2}-\d{2}"), nullable=False), # csv doesn't hold dtypes anyway
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

def output_check_duplicates(df: pd.DataFrame) -> None:
    assert not df.duplicated(subset=["category", "year", "quarter", "resource"]).any(), "output dataframe - repeated values"