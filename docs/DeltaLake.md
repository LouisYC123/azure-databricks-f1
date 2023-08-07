# Delta Lake

Delta Lake is a project originally developed by Databricks and then open sourced under the Linux Foundation license around late 2019.  

It's basically an open source storage layer that brings reliability to Data Lakes. It provides acid transactions, scalable metadata handling, and it unifies streaming as well as batch workloads. Delta Lakes run on top of Data Lakes, and they are fully compatible with Apache Spark APIs.

Delta Lake brings the Data Lakehouse Architecture to a data lake.

Lakehouse Architecture is aimed at bringing best of both Data Warehouse as well as Data Lakes. They have been designed to provide better BI support as well as Data Science and Machine Learning support.  

Similar to Data Lakes, we can ingest the operational and the external data into Delta Lakes. Delta Lake is nothing but a Data Lake with ACID transaction controls. Due to the ability to have ACID transactions, we can now combine streaming and batch workloads too and eliminate the need for a Lambda architecture. This data could then be transformed more efficiently without the need to rewrite enter partitions in cases of reprocessing data or rewriting Data Lakes, such as in the case of processing GDPR requests.  

Databricks Architecture has two stages, with the first stage transforming the raw data to provide a structure, perform any data cleansing and quality, etc., and return to a set of tables and they are called Silver tables.  
raw tables = bronze tables
processed / staging tables = silver tables
presentation / mart tables = Gold tables  

Delta Lake has been in existence since 2018 and rapidly evolving, but the performance still can't be compared to a Data Warehouse when it comes to reporting. So if you're looking for that level of performance, which you get from a Data Warehouse, you will still have to copy the data to a Data Warehouse.  

Delta Lakes handle all types of data and they still run on Cloud Object store, such as S3 and ADLs. So we have cost benefits there and they use open source file formats. And Delta Lake uses Parquet. They support all types of workloads, such as BI, Data Science and Machine Learning. They provide the ability to use BI tools directly on them. Most importantly, they provide ACID support, history and versioning. This helps us stop the unreliable data swamps being created, as in the case of Data Lakes. They provide better performance when compared to Data Lakes. And finally, they provide a very simple architecture. We do not need the Lambda architecture for streaming and batch workloads, and also we could potentially remove the need to copy the data from our Data Lake to the Data Warehouse.  

Similar to a Data Lake, Delta Lake also stores the data as Parquet files, which is open source format. Key difference here is that when storing the data in Parquet, Delta Lake also creates a Transaction Log alongside that file. This is what provides history, versioning, Acid transaction support, time travel, etc. This is a key difference between a Data Lake and a Delta Lake. The next layer is the Delta Engine, which is a Spark compatible query engine optimized for performance on Delta Lakes. It also uses Spark for munching through the transaction logs so that it could be distributed as well. We then have the Delta tables, which we can register with hive meta store to provide security via roles and access, governance, data integrity via constraints, as well as better optimizations. On the Delta Lake tables, we can run Spark workloads such as any kind of SQL transformations, Machine Learning workloads and streaming workloads as we do on a Data Lake.  

Most importantly, BI workloads can now directly access the Delta Lake. It is very simple to change from using a Parquet on a Data Lake to Delta Lake.
You simply have to change the format from Parquet to Delta on your spark.read or when you write the data in your writer, DataFrame writer APIs. Spark and Delta Engines take care of the rest for you.

/mnt/formula1lgdl/