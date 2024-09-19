# Structure and Handling of Data in Data Pipelines

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