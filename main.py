import pandas as pd 
import logging
from datetime import datetime
from src.scraper import (
    extract_from_link, 
    get_excel_link, 
    retrieve_filename, 
    confirm_new_file, 
    get_info
)
from src.transform import (
    extract_pie_df, 
    melt_df,
    clean_df,  
    add_dates, 
    add_filename, 
    save_csv
)
from src.data_integrity import (
    input_schema_validation, 
    input_checks, 
    output_schema_validation, 
    output_check_duplicates
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
        
        # gets response from provided url
        gov_response = extract_from_link(link=gov_link, retries=2)

        # finds the relevant excel link embedded in html content of response
        excel_link = get_excel_link(gov_response)

        # extracts file name
        excel_filename = retrieve_filename(excel_link)

        # checks if new - if not, exits function (logs that file exists)
        if not confirm_new_file(
            current_filename = excel_filename, 
            download_if_not_new = True, # Toggle On to see if code runs smoothly
            csv_location = csv_location
        ):
            logging.info("File data seems to have been ingested already")
            return # exits if a new file is NOT found
        
        # extracts response from excel url
        excel_response = extract_from_link(link=excel_link, retries=2)

        # gets the published date and relevant cells from relevant worksheet of excel file
        published_date_ts, info_df = get_info(excel_response)

        # validates input schema
        input_schema_validation(info_df)

        # integrity checks for excel file
        input_checks(info_df)

        # transformations
        final_df = (
            info_df
            .pipe(extract_pie_df)
            .pipe(melt_df)
            .pipe(clean_df)
            .pipe(add_dates, published_date = published_date_ts)
            .pipe(add_filename, filename = excel_filename)
        )

        # validates output schema
        output_schema_validation(final_df)
        output_check_duplicates(final_df) # easier to do here than with input

        # saves to final destination
        save_csv(final_df, csv_location)
    except Exception as e:
        logging.exception(f"Error seen: {e}")

if __name__ == "__main__":
    main()