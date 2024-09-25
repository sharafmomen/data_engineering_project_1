# Structure and Handling of Data in Data Pipelines

Petroineos is a large company with 201 to 500 employees. Likely, the data analysis team is also quite large, where there are sub-teams that focus on specific parts of the Delta Table. As Delta tables are columnar and make use of parquet files, and are best made use of with Delta Lake, my design sits on these factors. 

## Read Patterns:

I antitipate that many of the queries will be primarily by time. Therefore, the decision to partition by YEAR and subpartition by QUARTER would help to speed up queries. As different teams will focus on different products (crude oil, ngls) or categories (imports, exports, production), it'll be difficult to partition by any of these without compromising another focus' query speed and efficiency. Additionally, resource and category are low cardinality columns. Columnar storage would not be utilised efficienty if we partitioned by either of these columns. Therefore, I have decided to partition first by a column with a high cardinality - YEAR. 

I do believe there will be queries where filtering is done by category or resource, but since there aren't many values stored in each partition to begin with, filtering should be fairly efficient. This should draw on less resources, as the database engine can scan the content much quicker and return an answer. 

Additionally, there will be read queries that compare the performance of one resource over another, or look 
crude oil imports as a ratio of overall imports, etc. As this data is available within each partition already, the necessary calculations will be a lot faster and less memory will be used, as data from other partitions won't be needed to complete a single calculation. 

## Write Patterns:

Judging from the contents of the Cover page of the excel workbook, it seems that an update is sent monthly and towards the end of the month as well. There does seem to be variability, so the etl process could run every day during the last 10 days of the month, and I believe there would be new data once every month. Regardless, should there be much more data coming and to prevent issues with high throughput (more in terms of volume than velocity really), I would be making use of batch upserts. What this does is it takes a batch of data, like another delta file, and "merges" or upserts it into the delta table that is within delta. Why my design optimises for this is because not all data will be updated. Updates will likely focus on more recent years and quarters, so the partitioning I've provided will allow only relevant partitions to be accessed to rewrite the contents where applicable. 

## Concurrency:

With YEAR and QUARTER partitioning, as only relevant years will be tapped into, other partitions will be left alone during updates or upserts. This helps to provide more resources for read queries to make use of. There will be less conflicts and less of read and write queries from different teams and pipelines from colliding. 

A note I'd like to make is that I'd have the data pipeline set up to be triggered at night, in order to avoid being at the same time as several write queries during work hours. 

## Write Deduplication and Upserts:

Given that the data is partitioned by YEAR and QUARTER, duplicates would be caught within each partition itself. If there was a process to keep an eye out for duplicates or remove them, it can be done on the per partition level where necessary. 

UPSERTs would only target rows where the values have changed for a particular combination. As updates are primarily within the more recent years, UPSERTs would focus first on the YEAR and QUARTER partitioning, followed by a unique category and resource combination. Only when a combination does not exist will new rows be introduced, which is why UPSERTs are better are strategically introducing new information while handling potential duplications. 

## Additional 

### ML/Forecasting:
One drawback of the delta table and the current structure is that it cannot be used to query data for forecasting effectively, which would be one of the main needs of Petroineos. This is because it would introduce data leakage, or future data to a present situation where that data would not have otherwise been available. 

In a situation like this, you may be able to use the Delta API to read different versions of the table at different times, and join them one by one to a forecasting dataset where the published_date is less than the date the price of a resource that you're trying to forecast for. Maybe, as you use the Delta API, you may be able to take advantage of the YEAR and QUARTER partitioning. 

An easier and alternative scenario would be to take a snapshot and store it in another table, like on Redshift, which captures historical data well. This would be an INSERT, over the UPSERT mentioned above, but would allow for query performance increase as well, as everyone's query needs will be better distributed between a delta table and a redshift (or another delta) table. 

### Z-Ordering
To further utilise Delta Lake's features, alongside partitioning on YEAR and QUARTER, z-ordering can be used to order certain values in the rows in every partition based on what is accessed more often. This will quicken queries and reduce query time and increase query efficiency.  