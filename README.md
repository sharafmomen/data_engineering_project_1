# Petroineos_TakeHomeAssessment

## Aim

The aim for this assessment is to create a data pipeline that will check daily whether there is a new excel file for a particular categery of datasets, and if there's a new file, extract the information of economic indicators around resource and energy use (product, import, export, etc.). Then, I have to store it in a schema and explain the choice behind my design, and how I would structure the final Delta table. 

## What was done:

Pandas was use for data processing and storage. The output file was stored as a CSV file locally (please see code for the location). The entire process is a simplified method of showing the thinking behind the processing and design of a Delta table using PySpark. There is a file called DeltaTable.md that goes into depth about the design of the table, and the thinking behind it. 

## How it Works

### Scraping

A government link is provided as an argument, from which it will get a response from a GET request. It will dig through the html content to look for the appropriate excel link that we're interested in, and attempt to collect information from there. For each time a GET request is made, retry mechanisms are in place and logging is ready to take note of the nature of each failure. Within this section, we extract the published date, and have functions to check whether a file that's been detected is new. If the file found on the government website shown is not new, the program will terminate. If there is no useful data gained at any stage of a GET request, a Runtime error will stop the data pipeline run.

### Transforming

Transformations are made one step at a time, each major transformation packed into a function. All of these functions generally spit out a dataframe, which is why it was strung together with a pandas pipe function to show the flow of transformations. These transformations include removing unnecessary information, naming columns to something more meaningful, melting and making the table long format, etc. One of the most important transformations, aside from melting, is determining whether figures for a similar resource/material is an import figure, an export figure, or a production figure. 

### Data Integrity Checks

I have divided the data integrity checks into input and output. The input helps introduces basic checks to grab issues at the source with adequate logging, and the output checks help to ensure the final product is as expected. Additionally, there are certain checks that are harder to do on the input file until transformations are made, such as duplicates of a certain nature. These have been accounted for. These integrity tests include small functions that test for % null, minimum number of rows, whether there are an acceptable number of crude rows on crude oil, ngls, etc. and more. 

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

A set of unit tests have been made, which are dedicated to different source scripts. Simply run from root directory of project:

```
export PYTHONPATH=$PYTHONPATH:~/Petroineos_TakeHomeAssessment

pytest tests/*.py

```

At the moment, there should be 50 tests overall within the tests folder. 

## References:
- https://medium.com/@anastasia.prokaieva/why-anyone-should-know-delta-lake-if-you-work-with-data-b8c1e3636d60
- https://docs.databricks.com/en/machine-learning/feature-store/time-series.html 
- https://delta.io/blog/2023-02-01-delta-lake-time-travel/ 
- https://docs.databricks.com/en/delta/history.html
- https://community.databricks.com/t5/data-engineering/delta-live-tables-bulk-import-of-historical-data/td-p/8991 
- https://medium.com/@sentosa/schema-validation-for-a-pandas-data-frame-e452eb3e567e
- https://www.montecarlodata.com/blog-data-integrity-testing-examples/

## AI Assisted Code:
```
1. df['date_processed'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') == str(x)).all() 
(help with the general lambda function)

2. "date_published": pa.Column(pa.String, pa.Check.str_matches(r"\d{4}-\d{2}-\d{2}"), nullable=False) 
(how to use pandera.Check.str_matches)

3. error_columns = e.failure_cases['column'].unique() 
(wasn't able to find anywhere how to draw on columns for ONLY pinpointing problem columns in Pandera - for debugging)

4. mock_get.return_value.status_code = status_code 
(learning how to mock requests for unit testing)
```
