# Formula 1 Project


Data is sourced from ergast.com/mrd

## Project Requirements

### Extract Requirements
- Ingest all 8 files into the data lake
- Apply the right schema to the data, including column names, data types, etc. 
- Add some audit columns such as ingested date and the source from where the data has been received.
- Ingested data must be stored in columnar format, i.e. Parquet files.
- The data ingested should be available for all kind of workloads such as machine learning, further transformation
for reporting and also analytical workloads via SQL.
- The data ingestion logic must be able to handle incremental data, i.e. if we receive the data for just
one race, it should append the data in the lake rather than replacing all data in the lake.

### Transformation Requirements
- We need a table which provides the data joined from key data items that are required for our reporting.
- We also need audit columns in our transformed tables too. 
- Transformed data should be available for all workloads such as machine learning, BI reporting and SQL analytics.
- Transformed data should also be stored in columnar format, in this case as parquet files.
- Transformation logic should also be able to handle incremental data, similar to the ingestion requirements.

### BI Reporting Requirements
- We want to produce Driver standings as well as Constructor standings for the current race year, as well as every year from 1950 onwards.
- We want to find out the most dominant drivers and the teams over the last decade, as well as all time in Formula1.
- We then want to rank them in the order of their dominance.
- We also want to create various visualizations to see the period in which the driver or the teams were dominant, as well as their level of performance and the dominance. 
- We also want to create dashboards from these outcomes so that they can be shared with others. These dashboards will have to be created in Azure Databricks.

### Scheduling
- We want to schedule the pipelines to run at 10 p.m. every Sunday.
- If there is a race that weekend, the pipelines should process the data. Otherwise they should finish without failures.
- We want to have the ability to monitor the status of the pipeline executions, re-run failed pipelines as well as, we want to have the ability to set up alerts on failures.

### Other requirements
- We want to be able to delete individual records from the Data Lake to satisfy user privacy legislations such as GDPR.
- We want to be able to see the history of the data and ability to query the data based on time, which is time travel.
- Also, we want to be able to roll back the data in the Data Lake to a previous version, in cases of issues with the data in the current state.