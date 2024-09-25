import pandas as pd
import numpy as np
from datetime import datetime
import pytest
from src.transform import (
    retrieve_year_quarter,
    remove_note_data,
    retrieve_pie_category,
    nullify_category_if_not_pie,
    extract_pie_df,
    melt_df,
    clean_df, 
    add_dates, 
    add_filename, 
    save_csv, # skip
)

@pytest.mark.parametrize(
    "col, expected_output", 
    [
        ("2023 Q1", "2023 1"),
        ("2023-Q1", "2023 1"),
        ("2023 quarter1", "2023 1"),
        ("unknown string", "unknown string"), # no match
        ("202 Q1", "202 Q1"), # no match
    ],
)
def test_retrieve_year_quarter(col, expected_output):
    assert retrieve_year_quarter(col) == expected_output

@pytest.mark.parametrize(
    "col_val, expected_output", 
    [
        ("crude oil [note 2]", "crude oil "), 
        ("feedstocks [note 3]", "feedstocks "), 
        ("crude oil[note 2]", "crude oil"), 
        ("total supply", "total supply"),  # no removal
    ],
)
def test_remove_note_data(col_val, expected_output):
    assert remove_note_data(col_val) == expected_output

@pytest.mark.parametrize(
    "first_col_value, expected_category", 
    [
        ("indigenous production", "production"),  
        ("crude oil import", "import"),  
        ("export to ", "export"),  
        ("total supply", np.nan),  
    ],
)
def test_retrieve_pie_category(first_col_value, expected_category):
    result = retrieve_pie_category(first_col_value)
    if pd.isna(expected_category):
        assert pd.isna(result) 
    else:
        assert result == expected_category

@pytest.mark.parametrize(
    "row_data, expected_output", 
    [
        ({"resource": "production", "category": "production"}, "production"),  
        ({"resource": "imports", "category": "import"}, "import"),  
        ({"resource": "total demand", "category": "other"}, np.nan),  
    ],
)
def test_nullify_category_if_not_pie(row_data, expected_output):
    row = pd.Series(row_data)
    result = nullify_category_if_not_pie(row)
    if pd.isna(expected_output):
        assert pd.isna(result) 
    else:
        assert result == expected_output

def test_extract_pie_df():
    df = pd.DataFrame({"resource": ["production", "import", "export", "total supply"]})
    result_df = extract_pie_df(df)
    
    expected_df = pd.DataFrame({
        "resource": ["production", "import", "export", "total supply"],
        "category": ["production", "import", "export", "export"],
    })

    assert (result_df == expected_df).any().any()

def test_melt_df():
    df = pd.DataFrame({
        "resource": ["oil", "ngl"],
        "category": ["production", "import"],
        "2000 Q1": [10, 10],
        "2000 Q2": [10, 10]
    })
    
    result_df = melt_df(df)
    
    expected_df = pd.DataFrame({
        "resource": ["oil", "ngl", "oil", "ngl"],
        "category": ["production", "import", "production", "import"],
        "year_quarter": ["2000 1", "2000 1", "2000 2", "2000 2"],
        "figures": [10, 10, 10, 10]
    })
    
    assert (result_df == expected_df).any().any()

@pytest.mark.parametrize(
    "df_data, expected_output, expected_error", 
    [
        # ideal
        (
            {
                "year_quarter": ["2023 1", "2023 2"],
                "resource": ["oil [note 2]", "gas [note 3]"]
            },
            {
                "resource": ["oil", "gas"],
                "year": [2023, 2023],
                "quarter": [1, 2]
            },
            None
        ),
        # unexpected year and quarter format
        (
            {
                "year_quarter": ["2023 Q1", "2023 Q2"],
                "resource": ["oil", "gas"]
            },
            None,
            ValueError
        ),
        # removing trailing spaces
        (
            {
                "year_quarter": ["2023 1", "2023 2"],
                "resource": [" oil  ", " blah"]
            },
            {
                "resource": ["oil", "blah"],
                "year": [2023, 2023],
                "quarter": [1, 2]
            },
            None
        ),
        # year_quarter column doesn't exist
        (
            {
                "resource": ["oil", "gas"]
            },
            None,
            KeyError
        ),
    ]
)
def test_clean_df(df_data, expected_output, expected_error):
    df = pd.DataFrame(df_data)
    
    if expected_error:
        with pytest.raises(expected_error):
            clean_df(df)
    else:
        result_df = clean_df(df)
        expected_df = pd.DataFrame(expected_output)
        assert (result_df == expected_df).any().any()

@pytest.mark.parametrize(
    "df, published_date, expected_error", 
    [
        (
            pd.DataFrame({"resource": ["oil"], "figures": [1.5]}),
            datetime(2023, 7, 30),
            None
        ),
        # error
        (
            pd.DataFrame({"resource": ["oil"], "figures": [1.5]}),
            "not a date",
            AttributeError
        ),
        # error
        (
            pd.DataFrame({"resource": ["oil"], "figures": [1.5]}),
            1234567890,
            AttributeError
        ),
        # error
        (
            pd.DataFrame({"resource": ["oil"], "figures": [1.5]}),
            None,
            AttributeError
        ),
    ]
)
def test_add_dates(df, published_date, expected_error):
    if expected_error:
        with pytest.raises(expected_error):
            add_dates(df, published_date)
    else:
        result_df = add_dates(df, published_date)
        assert "date_published" in result_df.columns
        assert "date_processed" in result_df.columns
        assert result_df["date_published"].iloc[0] == "2023-07-30"

@pytest.mark.parametrize(
    "filename, expected_output_filename", 
    [
        ("valid.csv", "valid.csv"),  
        (77, 77),  
        (True, True),  
    ],
)
def test_add_filename(filename, expected_output_filename):
    df = pd.DataFrame({"resource": ["ngl", "crude"], "category": ["production", "production"]})
    result_df = add_filename(df, filename)
    assert result_df["filename"].iloc[0] == expected_output_filename
    assert result_df["filename"].nunique() == 1