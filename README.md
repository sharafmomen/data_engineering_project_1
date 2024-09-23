# Petroineos_TakeHomeAssessment

## Aim

The aim for this assessment is to create a data pipeline that will check daily whether there is a new excel file for a particular categery of datasets, and if there's a new file, extract resource supply and use data from it. 

## What was done:

Pandas was use for data processing and storage. The output file was stored as a CSV file locally (please see code for the location). The entire process is a simplified method of showing the thinking behind the processing and design of a Delta table using PySpark. There is a file called DeltaTable.md that goes into depth about the design of the table, and the thinking behind it. 

## Scraping

Although the title is along the lines of, "Supply and use of crude oil, natural gas liquids and feedstocks (ET 3.1 - quarterly)", reading the description and the actual link for the download shows that it's updated more frequently than expected. Here is the example download link:
"https://assets.publishing.service.gov.uk/media/66a76ff1ab418ab055592e8a/ET_3.1_JUL_24.xlsx"

Therefore, I have decided to read the month and year values towards the end of the link, which is nested deeply in the html of the provided link. 

## How to run the file

Steps:
- Have Python 3.12 installed
- Clone repo
- Go into the repo
- Create python environment
- pip install all the libraries in requirements.txt file
- run the main.py file

If you have Python 3.12, simply run the following in the root directory of the project step by step, replacing parts where you see fit:
```
git clone https://github.com/sharafmomen/Petroineos_TakeHomeAssessment.git

cd path_to/Petroineos_TakeHomeAssessment

python3 -m venv venv

source venv/bin/activate

pip install -r requirements.txt

python main.py
```

## Testing

A set of unit tests have been made, which are dedicated to different source scripts. Simply run

## References:
- https://medium.com/@anastasia.prokaieva/why-anyone-should-know-delta-lake-if-you-work-with-data-b8c1e3636d60
- https://docs.databricks.com/en/machine-learning/feature-store/time-series.html 
- https://delta.io/blog/2023-02-01-delta-lake-time-travel/ 
- https://docs.databricks.com/en/delta/history.html
- https://community.databricks.com/t5/data-engineering/delta-live-tables-bulk-import-of-historical-data/td-p/8991 
- https://medium.com/@sentosa/schema-validation-for-a-pandas-data-frame-e452eb3e567e
