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
from unittest.mock import patch
import pytest
from src.scraper import (
    extract_from_link, 
    get_excel_link, 
    retrieve_filename, 
    confirm_new_file, 
    get_info
)

@pytest.mark.parametrize(
    "link, status_code, retries, expected_error", 
    [
        # success
        ("http://gov_link.com", 200, 3, None),
        # fail
        ("http://gov_link.com", 404, 3, RuntimeError)
    ]
)
@patch('requests.get')
def test_extract_from_link(mock_get, link, status_code, retries, expected_error):
    mock_get.return_value.status_code = status_code
    
    if expected_error:
        with pytest.raises(expected_error):
            extract_from_link(link, retries, 1)
    else:
        response = extract_from_link(link, retries)
        assert response.status_code == 200

@pytest.mark.parametrize(
    "excel_link, expected_filename", 
    [
        # good
        ("http://gov_link.com/report.xlsx", "report.xlsx"),
        # good
        ("https://assets.gov.uk/media/12345678/ET_3.1_JUL_24.xlsx", "ET_3.1_JUL_24.xlsx"),
        # good
        ("ET_3.1_JUL_24.xlsx", "ET_3.1_JUL_24.xlsx"),
    ]
)
def test_retrieve_filename(excel_link, expected_filename):
    filename = retrieve_filename(excel_link)
    assert filename == expected_filename
