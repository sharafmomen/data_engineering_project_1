import pandas as pd
import numpy as np
from datetime import datetime
import pytest
from src.data_integrity import (
    input_schema_validation, 
    input_rows_check,
    input_nulls_check,
    input_duplicate_rows,
    input_temporal_integrity,
    input_allowable_negative_quantities,
    input_checks, 
    output_schema_validation
)

@pytest.mark.parametrize(
    "df_data, expected_error", 
    [
        # correct
        (
            {
                "0": ["ngl", "crude oil"],
                "2023 Quarter 1": [100, 100],
                "2023 Quarter 2": [200, 200]
            },
            None
        ),
        # incorrect
        (
            {
                "0": [1, 2],
                "2023 1st Quarter": [100, 100],
                "2023 2nd Quarter": [200, 200]
            },
            AssertionError
        ), 
        # incorrect
        (
            {
                "0": ["ngl", "crude oil"],
                "2023 1": ["string1", "string2"],
                "2023 2": [200, 200]
            },
            AssertionError
        )
    ]
)
def test_input_schema_validation(df_data, expected_error):
    df = pd.DataFrame(df_data)
    
    if expected_error:
        with pytest.raises(expected_error):
            input_schema_validation(df)
    else:
        input_schema_validation(df)

@pytest.mark.parametrize(
    "df_data, expected_error", 
    [
        # good
        (
            {
                "resource": ["production", "crude oil and ngls", "crude oil and ngls", "crude oil and ngls", 
                             "feedstock", "feedstock", "feedstock", "import", "export", "extra row"],
                "2023 1st Quarter": [100, 200, 200, 200, 200, 200, 200, 200, 200, 500]
            },
            None
        ),
         # good
        (
            {
                "resource": ["production", "feedstock", "crude oil", "crude oil", "crude oil", 
                             "ngls", "ngls", "ngls", "feedstock", "feedstock", "import", "export"],
                "2023 1st Quarter": [100, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200]
            },
            None
        ),
        # bad
        (
            {
                "resource": ["production", "crude oil", "ngls", "feedstock", "import", "export"]*2,
                "2023 1st Quarter": [100, 50, 20, 10, 80, 90]*2
            },
            AssertionError
        )
    ]
)
def test_input_rows_check(df_data, expected_error):
    df = pd.DataFrame(df_data)
    if expected_error:
        with pytest.raises(expected_error):
            input_rows_check(df)
    else:
        input_rows_check(df)

@pytest.mark.parametrize(
    "df_data, expected_error", 
    [
        # good
        (
            {
                "resource": ["production", "crude oil", "ngls", "feedstock", "import", "export"],
                "2023 Q1": [100]*6
            },
            None
        ),
        # bad
        (
            {
                "resource": ["production", "crude oil", "crude oil", "ngls", "import", "export"],
                "2023 Q1": [100]*6
            },
            AssertionError 
        )
    ]
)
def test_input_duplicate_rows(df_data, expected_error):
    df = pd.DataFrame(df_data)
    if expected_error:
        with pytest.raises(expected_error):
            input_duplicate_rows(df)
    else:
        input_duplicate_rows(df)

##### MORE TESTS TO BE PLACED HERE for temporal integrity, nulls, allowable negative quantities, etc. 

@pytest.mark.parametrize(
    "df_data, expected_error", 
    [
        # ideal
        (
            {
                "resource": [
                    "production", "feedstock", "crude oil", "crude oil", "crude oil", 
                    "ngls", "ngls", "ngls", "feedstock", "feedstock", "import", "export"
                ],
                "2023 1st Quarter": [10, 20, 50, 60, 70, 50, 30, 40, 10, 50, 80, 90],
                "2023 2nd Quarter": [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
            },
            None  
        ),
        # less than 10 rows
        (
            {
                "resource": ["production", "crude oil", "ngls", "feedstock"],
                "2023 Q1": [100, 50, 20, 10],
                "2023 Q2": [100, 50, 20, 10]
            },
            AssertionError  
        )
    ]
)
def test_input_checks(df_data, expected_error):
    df = pd.DataFrame(df_data)
    
    if expected_error:
        with pytest.raises(expected_error):
            input_checks(df)
    else:
        input_checks(df)

@pytest.mark.parametrize(
    "df_data, expected_exception", 
    [
        # valid data
        (
            {
                "resource": ["oil"],
                "category": ["production"],
                "figures": [100.0],
                "year": [2023],
                "quarter": [1],
                "date_published": ["2023-07-30"],
                "date_processed": [datetime.now().replace(microsecond=0)],
                "filename": ["file.csv"]
            },
            None
        ),
        # wrong datetime format
        (
            {
                "resource": ["oil"],
                "category": ["production"],
                "figures": [100.0],
                "year": [2023],
                "quarter": [1],
                "date_published": ["2023-07-30"],
                "date_processed": [datetime.now()],
                "filename": ["file.csv"]
            },
            AssertionError
        ),
        # missing column (year)
        (
            {
                "resource": ["oil"],
                "category": ["production"],
                "figures": [100.0],
                "quarter": [1],
                "date_published": ["2023-07-30"],
                "date_processed": [datetime.now().replace(microsecond=0)],
                "filename": ["file.csv"]
            },
            AssertionError
        ),
        # wrong datatype for 'figures' (should be float)
        (
            {
                "resource": ["oil"],
                "category": ["production"],
                "figures": ["one hundred"],
                "year": [2023],
                "quarter": [1],
                "date_published": ["2023-07-30"],
                "date_processed": [datetime.now().replace(microsecond=0)],
                "filename": ["file.csv"]
            },
            ValueError
        ),
        # wrong date format in 'date_published'
        (
            {
                "resource": ["oil"],
                "category": ["production"],
                "figures": [100.0],
                "year": [2023],
                "quarter": [1],
                "date_published": ["30-07-2023"],
                "date_processed": [datetime.now().replace(microsecond=0)],
                "filename": ["file.csv"]
            },
            ValueError
        ),
        # extra column
        (
            {
                "resource": ["oil"],
                "category": ["production"],
                "figures": [100.0],
                "year": [2023],
                "quarter": [1],
                "date_published": ["2023-07-30"],
                "date_processed": [datetime.now().replace(microsecond=0)],
                "filename": ["file.csv"],
                "extra_column": ["unexpected"]
            },
            AssertionError
        )
    ]
)
def test_output_schema_validation(df_data, expected_exception):
    df = pd.DataFrame(df_data)
    print(df.info())
    if expected_exception:
        with pytest.raises(expected_exception):
            output_schema_validation(df)
    else:
        output_schema_validation(df)