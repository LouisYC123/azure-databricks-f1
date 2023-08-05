# Spark Transformations

## filter

```df.filter()```  

e.g
```df.filter(df.age > 3)```

you can use sql or python syntax

## Join 

There are a number of join types available in Spark
```df.join(other_df, on=''m how='')```


https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/27517898#overview


#### Anti Joins
The anti join is going to give you is everything on the left dataframe which is not found on the right dataframe.