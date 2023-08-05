# Hive Meta Store

Data is stored in a Data Lake as files of varying types such as CSV, JSON and Parquet. In order for Spark to treat this data as tables and columns, we need to register this data in a meta store. Meta Store is nothing but a storage for storing the metadata about the data files. For example, things like the location of the file, the format of the data, column names, etc. Spark uses a meta store provided by the Apache Hive project for this, which is called Hive Meta Store. Hive Meta Store is the most commonly used metadata in the Data Lake space. When it comes to choosing the storage for hive meta store, we have a choice. We can either choose the Default Databricks managed Meta Store or the External Storage option of your own. So you've got a choice between Azure SQL, MySQL, MariaDB and a few others. Once we've registered our tables in the hive meta store, we can then use Spark SQL to access these tables
like we would in a relational database.  

Just to summarize, the data is usually stored in an object storage which is ADLs, in our case. Hive Meta Store keeps the information about the file, such as the location, name of the file, table, column, etc..  

When you run a Spark SQL command, Spark uses the meta store to apply the schema and access the files accordingly, as if we are accessing any relational table.  

At Databricks Workspace can have a number of databases, they are also referred as schemas. Within a database, they can have a number of tables and views.
Tables are basically structures given to the data stored in an object storage.  

There are two types of tables in Spark. The first one is called the Managed Table and the other one is called External or Unmanaged Table.  

In the case of Managed Table, Spark maintains both the metadata in Hive Meta Store and also the data files associated with the table, which is stored in ADLs, in our case. In the case of External Tables, Spark only manages the metadata and we manage the data files.  