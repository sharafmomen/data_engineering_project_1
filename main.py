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
    melt_df, 
    filter_df
)

def main():
    top_link_response = extract_from_top_link()
    excel_link = get_excel_link(top_link_response)
    filter_from, info_df = get_info(excel_link)
    melted_df = melt_df(info_df)
    filtered_df = filter_df(melted_df, filter_from)
    print(filtered_df)

if __name__ == "__main__":
    main()