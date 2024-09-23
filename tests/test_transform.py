import pytest
from dateutil import parser
from ..src.transform import (
    melt_df, 
    filter_df, 
    add_dates
)

@pytest.mark.parametrize(
    "col_name, outcome",
    [
        ("weekly_premium_v8", ),
        ("monthly_premium_v1", ),
        ("monthly_premium_no_trial_v10", ),
        ("5_boost_v3", ),
    ],
)
def test_retrieve_quarter():
    1