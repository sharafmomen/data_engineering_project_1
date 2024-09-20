import pandas as pd 
from bs4 import BeautifulSoup
import pandera as pa 
from src.scraper import (
    extract_from_top_link, 
    get_excel_link, 
    get_info
)
from src.transform import (
    retrieve_year_quarter, 
    melt_and_filter_rows
)

def main():
    top_link_response = extract_from_top_link()
    excel_link = get_excel_link(top_link_response)
    filter_from, info_df = get_info(excel_link)
    filtered_df = melt_and_filter_rows(info_df, filter_from)

if __name__ == "__main__":
    main()