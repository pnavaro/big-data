---
jupytext:
  encoding: '# -*- coding: utf-8 -*-'
  formats: ipynb
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.12
    jupytext_version: 1.6.0
kernelspec:
  display_name: big-data
  language: python
  name: big-data
---

# Spark dataframes on HDFS

New York City Taxi Cab Trip

We look at [the New York City Taxi Cab dataset](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml). This includes every ride made in the city of New York since 2009.

On [this website](http://chriswhong.github.io/nyctaxi/) you can see the data for one random NYC yellow taxi on a single day.

On [this post](http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/), you can see an analysis of this dataset. Postgres and R scripts are available on [GitHub](https://github.com/toddwschneider/nyc-taxi-data).

+++

## Loading the data

Normally we would read and load this data into memory as a Pandas dataframe.  However in this case that would be unwise because this data is too large to fit in RAM.

The data can stay in the hdfs filesystem but for performance reason we can't use the csv format. The file is large (32Go) and text formatted. Data Access is very slow.

+++

You can convert csv file to parquet with Spark.
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .master("spark://svmass2.mass.uhb.fr:7077") \
        .config('spark.hadoop.parquet.enable.summary-metadata', 'true') \
        .config("spark.cores.max", "10") \
        .getOrCreate()
df = spark.read.csv(
"hdfs://svmass2.mass.uhb.fr:54310/user/datasets/nyc-tlc/2009/yellow_tripdata_2009-01.csv", 
                    header="true",inferSchema="true")
df.write.parquet("hdfs://svmass2.mass.uhb.fr:54310/user/navaro_p/nyc-taxi/2019-01.parquet")
spark.stop()
```

To read multiple files
```py
spark.read.format("csv").option("header", "true").load("../Downloads/*.csv")
```

+++

## Spark Cluster

A Spark cluster is available and described on this [web interface](http://svmass2.mass.uhb.fr:8080)

![](images/cluster-overview.png)

```py
from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .master('spark://svmass2.mass.uhb.fr:7077') \
        .getOrCreate()
spark
```

The SparkSession is connected to the Spark’s own standalone cluster manager (It is also possible to use YARN). The manager allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (Python file) to the executors. Finally, tasks are sent to the executors to run.

Spark can access to files located on hdfs and it is also possible to access to local files. Example:

```py
df = spark.read.parquet('file:///home/navaro_p/nyc-taxi/2016.parquet')
```

### Exercise

- Pick a year and read and convert csv files to parquet in your hdfs homedirectory.
- **Don't run the python code inside a notebook cell**. Save a python script and launch it from a terminal instead.
In Jupyter notebook you won't see any progress or information if error occurs.
- Use the [`spark-submit`](https://spark.apache.org/docs/latest/submitting-applications.html) command shell to run your script on the cluster.
- You can control the log with 
```py
spark.sparkContext.setLogLevel('ERROR')
```
Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

**Try your script with a single file before to do it for a whole year.**

**Read carefully the script given above, don't submit it as is. You have to change some
part of this code**

+++

## Some examples that can be run on the cluster

- Here we read the NYC taxi data files of year 2016 and select some variables.

```py
columns = ['tpep_pickup_datetime', 'passenger_count', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'tip_amount', 'total_amount']

df = (spark.read.parquet('hdfs://svmass2.mass.uhb.fr:54310/user/navaro/nyc-taxi/2016.parquet').select(*columns))
```

- Sum the total number of passengers
```py
df.agg({'passenger_count': 'sum'}).collect()
```

- Average number of passenger per trip`
```py
df.agg({'passenger_count': 'avg'}).collect()
```

- How many trip with 0,1,2,3,...,9 passenger`
```py
df.groupby('passenger_count').agg({'*': 'count'}).collect()
```

## Exercise

How well people tip based on the number of passengers in a cab.  To do this you have to:

1.  Remove rides with zero fare
2.  Add a new column `tip_fraction` that is equal to the ratio of the tip to the fare
3.  Group by the `passenger_count` column and take the mean of the `tip_fraction` column.

### Cheat Sheets and documentation

- [Spark DataFrames in Python](https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf)
- [Spark in Python](http://datacamp-community.s3.amazonaws.com/4d91fcbc-820d-4ae2-891b-f7a436ebefd4)
-  https://spark.apache.org/docs/latest/api/python/pyspark.sql.html

Use the [PySpark API](https://spark.apache.org/docs/latest/api/python/index.html).

- **Write a python program and use `spark-submit`**
- **Read the parquet files instead of csv files** 
- **Don't forget spark.stop() at the end of the script**

+++

## Hints

- How to remove rows
```python
df = df.filter(df.name == 'expression')
```

- How to make new columns
```python
df = df.withColumn('var2', df.var0 + df.var1)
```

- How to do groupby-aggregations
```python
df.groupBy(df.name).agg({'column-name': 'avg'})
```

When you want to collect the result of your computation, finish with the `.collect()` method.

+++

### Exercices 

1. Plot the tip as a function of the hour of day and the day of the week?
2. Investigate the `payment_type` column.  See how well each of the payment types correlate with the `tip_fraction`.  Did you find anything interesting?  Any guesses on what the different payment types might be?  If you're interested you may be able to find more information on the [NYC TLC's website](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
3.  How quickly can you get a taxi cab for a particular day of the year?  How about for a particular hour of that day?
