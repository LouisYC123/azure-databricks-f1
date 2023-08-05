# Databricks Workflows

Spark lets us reuse a notebook in another notebook using the %run magic command.

so if we had some variables stored in a notebook called "configuration", we could run:

``` %run "../includes/configuration" ```


## Passing Parameters (widgets)

Parameters help us reuse an entire notebook.  

For example, if you processing data from two different sources with same characteristics, instead of writing two different notebooks, you can write one notebook and send the datasource name as a parameter and store that against the data.  

you can parameterized all our notebooks with the datasource name as a parameter and send the value at runtime. Widgets in Databricks help us do that.  
https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/27517420#overview  

## Workflows
Databricks offers jobs that you can schedule to run a specific time are a regular interval, which is great, but it lacks a lot of functionality that you would normally get with any kind of scheduling tools. For example, you can't create dependencies between jobs. To address that Databricks offers and utility called notebook utility, which lets you create notebook workflow.

chaining notebooks into a workflow:  
https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/27517424#overview
