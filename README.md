# Formula 1 Project

Data is sourced from ergast.com/mrd  

![AzureF1Architecture drawio-2](https://github.com/LouisYC123/azure-databricks-f1/assets/97873724/d1e65858-6cb2-4e9a-89df-aa8afe0a25b0)


## Project Specification

### Extract 
- Ingests all 8 files into the data lake
- Applies a schema to the data, including column names, data types, etc. 
- Adds audit columns such as ingested date and the source from where the data has been received.
- Stores data in columnar format, i.e. Parquet files.

### Transformation 
- Joins key data as required for reporting.
- Provides audit columns
- Stores Transformed data as parquet files.
- Transformation logic handles both initial load and incremental data loads

### BI Reporting 
- Provides analysis for Driver standings as well as Constructor standings for the current race year, as well as every year from 1950 onwards.
- Finds the most dominant drivers and the teams over the last decade, as well as all time in Formula1, and ranks them in the order of their dominance.

### Scheduling
- Pipelines are scheduled to run at 10 p.m. every Sunday.
- If there is a race that weekend, pipelines process new data, otherwise they finish without failures.

