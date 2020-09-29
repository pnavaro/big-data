---
jupytext:
  encoding: '# -*- coding: utf-8 -*-'
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

# Spark DataFrames

- Enable wider audiences beyond “Big Data” engineers to leverage the power of distributed processing
- Inspired by data frames in R and Python (Pandas)
- Designed from the ground-up to support modern big
data and data science applications
- Extension to the existing RDD API

## References
- [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Introduction to DataFrames - Python](https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html)
- [PySpark Cheat Sheet: Spark DataFrames in Python](https://www.datacamp.com/community/blog/pyspark-sql-cheat-sheet)

+++

### DataFrames are :
- The preferred abstraction in Spark
- Strongly typed collection of distributed elements 
- Built on Resilient Distributed Datasets (RDD)
- Immutable once constructed

### With Dataframes you can :
- Track lineage information to efficiently recompute lost data 
- Enable operations on collection of elements in parallel

### You construct DataFrames
- by parallelizing existing collections (e.g., Pandas DataFrames) 
- by transforming an existing DataFrames
- from files in HDFS or any other storage system (e.g., Parquet)

### Features
- Ability to scale from kilobytes of data on a single laptop to petabytes on a large cluster
- Support for a wide array of data formats and storage systems
- Seamless integration with all big data tooling and infrastructure via Spark
- APIs for Python, Java, Scala, and R

+++

### DataFrames versus RDDs
- Nice API for new users familiar with data frames in other programming languages.
- For existing Spark users, the API will make Spark easier to program than using RDDs
- For both sets of users, DataFrames will improve performance through intelligent optimizations and code-generation

+++

## PySpark Shell

**Run the Spark shell:**

~~~ bash
pyspark
~~~

Output similar to the following will be displayed, followed by a `>>>` REPL prompt:

~~~
Python 3.6.5 |Anaconda, Inc.| (default, Apr 29 2018, 16:14:56)
[GCC 7.2.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
2018-09-18 17:13:13 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Python version 3.6.5 (default, Apr 29 2018 16:14:56)
SparkSession available as 'spark'.
>>>
~~~

Read data and convert to Dataset

~~~ py
df = sqlContext.read.csv("/tmp/irmar.csv", sep=';', header=True)
~~~

~~~
>>> df2.show()
+---+--------------------+------------+------+------------+--------+-----+---------+--------+
|_c0|                name|       phone|office|organization|position|  hdr|    team1|   team2|
+---+--------------------+------------+------+------------+--------+-----+---------+--------+
|  0|      Alphonse Paul |+33223235223|   214|          R1|     DOC|False|      EDP|      NA|
|  1|        Ammari Zied |+33223235811|   209|          R1|      MC| True|      EDP|      NA|
.
.
.
| 18|    Bernier Joachim |+33223237558|   214|          R1|     DOC|False|   ANANUM|      NA|
| 19|   Berthelot Pierre |+33223236043|   601|          R1|      PE| True|       GA|      NA|
+---+--------------------+------------+------+------------+--------+-----+---------+--------+
only showing top 20 rows
~~~

+++

## Transformations, Actions, Laziness

Like RDDs, DataFrames are lazy. Transformations contribute to the query plan, but they don't execute anything.
Actions cause the execution of the query.

### Transformation examples
- filter
- select
- drop
- intersect 
- join
### Action examples
- count 
- collect 
- show 
- head
- take

+++

## Creating a DataFrame in Python

```{code-cell} ipython3
import sys, subprocess
import os

os.environ["PYSPARK_PYTHON"] = sys.executable
```

```{code-cell} ipython3
from pyspark import SparkContext, SparkConf, SQLContext
# The following three lines are not necessary
# in the pyspark shell
conf = SparkConf().setAppName("people").setMaster("local[*]") 
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
```

```{code-cell} ipython3
df = sqlContext.read.json("data/people.json") # get a dataframe from json file

df.show(24)
```

## Schema Inference

In this exercise, let's explore schema inference. We're going to be using a file called `irmar.txt`. The data is structured, but it has no self-describing schema. And, it's not JSON, so Spark can't infer the schema automatically. Let's create an RDD and look at the first few rows of the file.

```{code-cell} ipython3
rdd = sc.textFile("data/irmar.csv")
for line in rdd.take(10):
  print(line)
```

## Hands-on Exercises

You can look at the <a href="http://spark.apache.org/docs/2.3.1/api/python/index.html" target="_blank">DataFrames API documentation</a> 

Let's take a look to file "/tmp/irmar.csv". Each line consists 
of the same information about a person:

* name
* phone
* office
* organization
* position 
* hdr
* team1
* team2

```{code-cell} ipython3
from collections import namedtuple

rdd = sc.textFile("data/irmar.csv")

Person = namedtuple('Person', ['name', 'phone', 'office', 'organization', 
                               'position', 'hdr', 'team1', 'team2'])
def str_to_bool(s):
    if s == 'True': return True
    return False

def map_to_person(line):
    cols = line.split(";")
    return Person(name         = cols[0],
                  phone        = cols[1],
                  office       = cols[2],
                  organization = cols[3],
                  position     = cols[4], 
                  hdr          = str_to_bool(cols[5]),
                  team1        = cols[6],
                  team2        = cols[7])
    
people_rdd = rdd.map(map_to_person)
df = people_rdd.toDF()
```

```{code-cell} ipython3
df.show()
```

### Schema

```{code-cell} ipython3
df.printSchema()
```

### display

```{code-cell} ipython3
display(df)
```

### select

```{code-cell} ipython3
df.select(df["name"], df["position"], df["organization"])
```

```{code-cell} ipython3
df.select(df["name"], df["position"], df["organization"]).show()
```

### filter

```{code-cell} ipython3
df.filter(df["organization"] == "R2").show()
```

### filter + select

```{code-cell} ipython3
df2 = df.filter(df["organization"] == "R2").select(df['name'],df['team1'])
```

```{code-cell} ipython3
df2.show()
```

### orderBy

```{code-cell} ipython3
(df.filter(df["organization"] == "R2")
   .select(df["name"],df["position"])
   .orderBy("position")).show()
```

### groupBy

```{code-cell} ipython3
df.groupby(df["hdr"])
```

```{code-cell} ipython3
df.groupby(df["hdr"]).count().show()
```

WARNING: Don't confuse GroupedData.count() with DataFrame.count(). GroupedData.count() is not an action. DataFrame.count() is an action.

```{code-cell} ipython3
df.filter(df["hdr"]).count()
```

```{code-cell} ipython3
df.filter(df['hdr']).select("name").show()
```

```{code-cell} ipython3
df.groupBy(df["organization"]).count().show()
```

### Exercises

- How many teachers from INSA (PR+MC) ?
- How many MC in STATS team ?
- How many MC+CR with HDR ?
- What is the ratio of student supervision (DOC / HDR) ?
- List number of people for every organization ?
- List number of HDR people for every team ?
- Which team contains most HDR ?
- List number of DOC students for every organization ?
- Which team contains most DOC ?
- List people from CNRS that are neither CR nor DR ?

```{code-cell} ipython3
sc.stop()
```

```{code-cell} ipython3

```
