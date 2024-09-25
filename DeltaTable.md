# Structure and Handling of Data in Data Pipelines

Petroineos is a large company with 201 to 500 employees. Likely, the data analysis team is also quite large, where there are sub-teams that focus on specific parts of the Delta Table. Some might focus specifically on exports,  

## Read Patterns:

## Write Patterns:

## Write Concurrency:

## Write Deduplication and Upserts:
- It's important to make use of ELT pipelines, as it allows data to be stored in a data lake (like AWS S3) prior to it being processed more effectively and stored in the final format in a data warehouse (like AWS Redshift). While I aim to create a record of rows that are most relevant in the CSV file, I will create a temporary table of this CSV file in Redshift, and then UPSERT this into the final table. 
- Details:
    - Relevant rows in the CSV file, which is based on information in the "Cover Sheet" tab of the excel file, which explains which rows are affected. 
    - Temporary Redshift table is created based on the CSV file. This table will be truncated (or deleted) at the start of each ETL run, and then filled with the values of a current CSV file. 
    - The temporary table will then be upserted into the final table via UPDATE and INSERT statements, which prevents duplicates and updates rows. Redshift doesn't have a direct UPSERT option, but since it's columnar and designed for batch processes, two commands like an UPDATE and INSERT statement would be efficient. 
- The reason I don't directly UPSERT from the CSV file in the S3 file is because that is not an available option. There are also benefits to this approach as well:
    
    - Should an error occur in the load into the final Redshift table, there is a temporary table that can be 
    - If the final Redshift table accidentally got deleted, a python script can easily iterate through the S3 CSV files in the right order (which is why naming convention is important), and continually UPSERT in batches. 
- A note on locking and 

```SQL
COPY my_table
FROM 's3://your-bucket-name/path/to/your-file.csv'
IAM_ROLE 'arn:aws:iam::your-iam-role-id:role/your-role-name'
DELIMITER ',' 
IGNOREHEADER 1
CSV;
```

## Applications:
- Reporting:
    - This is the simplest application for the final Redshift table. Since it's all UPSERTs, 
- ML/Forecasting:
    - Since the final table in this project uses upserts to update the final table, it's important to note that there will be information that will be present when it wasn't available, should you use time-lagged variables. In a situation like this, what should follow each ETL run into this final Redshift table is another table (called historical_snapshots) with a snapshot of data available at a particular point in time. This would then be appended to the historical_snapshots table. The Example columns could be:
        - publish_date, resource, quarter_1_ago, ngls_quarter_2_ago, ..., quarter_n_ago. 

## Alternative Scenarios:
- Should you want to do both the reporting and feature engineering for forecasting from the same table, you'd have to completely scrap the idea of upserting, and make the journey from S3 to Redshift purely INSERTs. 
- Reporting:
    - For the case where you'd want to do reporting, for each year/quarter combination, you'd need to get the latest results. You'd have to RANK and PARTITION by year, quarter and resource in DESC order, then get the values where RANK = 1. 
    - An alternative is to make the Delta file have ALL the year and quarter values, rather than those are are likely updated - this is paired with INSERTs. This is more storage intensive (both in S3 and Redshift), but the filtering will be easier. Simply report values where etl_date = max(etl_date). 