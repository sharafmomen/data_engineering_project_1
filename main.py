import pandas as pd 
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import pandera as pa 
from src.scraper import (
    extract_from_link, 
    get_excel_link, 
    retrieve_filename, 
    confirm_new_file, 
    get_info, 
    input_schema_validation
)
from src.transform import (
    melt_df,
    clean_df,  
    add_dates, 
    output_schema_validation, 
    add_filename, 
    save_csv
)

logging.basicConfig(
    filename=f"logs/{datetime.today().strftime('%Y-%m-%d')}_log.log",
    encoding="utf-8",
    level=logging.INFO,
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

gov_link = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
csv_location = "submit_csv/" # can also make this "" if you'd like to store in root directory

def main():
    try:
        gov_response = extract_from_link(link=gov_link, retries=2)
        excel_link = get_excel_link(gov_response)
        excel_filename = retrieve_filename(excel_link)
        if not confirm_new_file(
            current_filename = excel_filename, 
            download_if_not_new = False, # Toggle On to see if code runs smoothly
            csv_location = csv_location
        ):
            logging.info("File data seems to have been ingested already")
            return # exits if a new file is NOT found
        excel_response = extract_from_link(link=excel_link, retries=2)
        published_date_ts, info_df = get_info(excel_response)
        input_schema_validation(info_df)
        final_df = (
            info_df
            .pipe(melt_df)
            .pipe(clean_df)
            .pipe(add_dates, published_date = published_date_ts)
            .pipe(add_filename, filename = excel_filename)
        )
        output_schema_validation(final_df)
        save_csv(final_df, csv_location)
    except Exception as e:
        print(e)
        logging.exception(f"Error seen: {e}")

def main_test(inp=None):
    print(f"{inp}blahblah")

if __name__ == "__main__":
    main()
    # main_test("")