import pandas as pd 
from bs4 import BeautifulSoup
import pandera as pa 
from src.scraper import (
    extract_from_top_link, 
    get_excel_link, 
    get_info
)
from src.transform import (
    melt_df,
    clean_df,  
    add_dates, 
    save_csv
)

def main():
    top_link_response = extract_from_top_link()
    excel_link = get_excel_link(top_link_response)
    published_date_ts, info_df = get_info(excel_link)
    final_df = (
        info_df
        .pipe(melt_df)
        .pipe(clean_df)
        .pipe(add_dates, published_date = published_date_ts)
    )
    print(final_df)
    save_csv(final_df, "submit_csv/")

if __name__ == "__main__":
    main()