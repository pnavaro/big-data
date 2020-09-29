---
jupytext:
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

# Hadoop File Formats

[Format Wars: From VHS and Beta to Avro and Parquet](http://www.svds.com/dataformats/)

+++

## Feather

For light data, it is recommanded to use [Feather](https://github.com/wesm/feather). It is a fast, interoperable data frame storage that comes with bindings for python and R.

Feather uses also the Apache Arrow columnar memory specification to represent binary data on disk. This makes read and write operations very fast.

```{code-cell} ipython3
import feather
import pandas as pd
import numpy as np
arr = np.random.randn(10000) # 10% nulls
arr[::10] = np.nan
df = pd.DataFrame({'column_{0}'.format(i): arr for i in range(10)})
feather.write_dataframe(df, 'test.feather')
```

```{code-cell} ipython3
df = pd.read_feather("test.feather")

df.head()
```

## Parquet file format

[Parquet format](https://github.com/apache/parquet-format) is a common binary data store, used particularly in the Hadoop/big-data sphere. It provides several advantages relevant to big-data processing:

- columnar storage, only read the data of interest
- efficient binary packing
- choice of compression algorithms and encoding
- split data into files, allowing for parallel processing
- range of logical types
- statistics stored in metadata allow for skipping unneeded chunks
- data partitioning using the directory structure

+++

## Apache Arrow

[Arrow](https://arrow.apache.org/docs/python/) is a columnar in-memory analytics layer designed to accelerate big data. It houses a set of canonical in-memory representations of flat and hierarchical data along with multiple language-bindings for structure manipulation.

https://arrow.apache.org/docs/python/parquet.html

The Apache Parquet project provides a standardized open-source columnar storage format for use in data analysis systems. It was created originally for use in Apache Hadoop with systems like Apache Drill, Apache Hive, Apache Impala, and Apache Spark adopting it as a shared standard for high performance data IO.

Apache Arrow is an ideal in-memory transport layer for data that is being read or written with Parquet files. [PyArrow](https://arrow.apache.org/docs/python/) includes Python bindings to read and write Parquet files with pandas.

Example:
```py
import pyarrow as pa

hdfs = pa.hdfs.connect('svmass2.mass.uhb.fr', 54311, 'navaro_p')
```

```{code-cell} ipython3
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
```

```{code-cell} ipython3
df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                   'two': ['foo', 'bar', 'baz'],
                   'three': [True, False, True]},
                   index=list('abc'))
table = pa.Table.from_pandas(df)
```

```{code-cell} ipython3
pq.write_table(table, 'example.parquet')
```

```{code-cell} ipython3
table2 = pq.read_table('example.parquet')
```

```{code-cell} ipython3
table2.to_pandas()
```

```{code-cell} ipython3
pq.read_table('example.parquet', columns=['one', 'three'])
```

```{code-cell} ipython3
pq.read_pandas('example.parquet', columns=['two']).to_pandas()
```
