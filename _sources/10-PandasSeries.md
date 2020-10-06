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

+++ {"slideshow": {"slide_type": "slide"}}

# Pandas Series

![pandas](images/pandas.png "Pandas Logo")


- Started by Wes MacKinney with a first release in 2011.
- Based on NumPy, it is the most used library for all things data.
- Motivated by the toolbox in R for manipulating data easily.
- A lot of names in Pandas come from R world.
- It is Open source (BSD)

https://pandas.pydata.org/

+++ {"slideshow": {"slide_type": "slide"}}


```python
import pandas as pd
```

"*Pandas provides high-performance, easy-to-use data structures 
and data analysis tools in Python*"

- Self-describing data structures
- Data loaders to/from common file formats
- Plotting functions
- Basic statistical tools.

```{code-cell} ipython3
---
slideshow:
  slide_type: skip
---
%matplotlib inline
%config InlineBackend.figure_format = 'retina'
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
pd.set_option("display.max_rows", 8)
plt.rcParams['figure.figsize'] = (9, 6)
```

+++ {"slideshow": {"slide_type": "slide"}}

## [Series](https://pandas.pydata.org/pandas-docs/stable/dsintro.html#series)

- A Series contains a one-dimensional array of data, *and* an associated sequence of labels called the *index*.
- The index can contain numeric, string, or date/time values.
- When the index is a time value, the series is a [time series](https://en.wikipedia.org/wiki/Time_series).
- The index must be the same length as the data.
- If no index is supplied it is automatically generated as `range(len(data))`.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
pd.Series([1,3,5,np.nan,6,8], dtype=np.float64)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
pd.Series(index=pd.period_range('09/11/2017', '09/18/2017', freq="D"), dtype=np.int8)
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise
- Create a text with `lorem` and count word occurences with a `collection.Counter`. Put the result in a `dict`.

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise
- From the results create a Pandas series name latin_series with words in alphabetical order as index.

```{code-cell} ipython3
df = pd.Series(result)
df
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise

- Plot the series using 'bar' kind.

+++ {"slideshow": {"slide_type": "fragment"}}

### Exercise
- Pandas provides explicit functions for indexing `loc` and `iloc`.
    - Use `loc` to display the number of occurrences of 'dolore'.
    - Use `iloc` to diplay the number of occurrences of the last word in index.

+++

### Exercise
- Sort words by number of occurrences.
- Plot the Series.

+++

### Full globe temperature between 1901 and 2000.

We read the text file and load the results in a pandas dataframe. 
In cells below you need to clean the data and convert the dataframe to a time series.

```{code-cell} ipython3
import os
here = os.getcwd()

filename = os.path.join(here,"data","monthly.land.90S.90N.df_1901-2000mean.dat.txt")

df = pd.read_table(filename, sep="\s+", 
                   names=["year", "month", "mean temp"])
df
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise
- Insert a third column with value one named "day" with `.insert`.
- convert df index to datetime with `pd.to_datetime` function.
- convert df to Series containing only "mean temp" column.

+++

### Exercise 
- Display the beginning of the file with `.head`.

+++

### Exercise 
- Display the end of the file with `.tail`.

+++

In the dataset, -999.00 was used to indicate that there was no value for that year.

### Exercise

- Display values equal to -999 with `.values`. 
- Replace the missing value (-999.000) by `np.nan` 

+++

Once they have been converted to np.nan, missing values can be removed (dropped).

### Exercise 
- Remove missing values with `.dropna`.

+++

### Exercise
- Generate a basic visualization using `.plot`.

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise

Convert df index from timestamp to period is more meaningfull since it was measured and averaged over the month. Use `to_period` method.

+++ {"slideshow": {"slide_type": "slide"}}

## Resampling

Series can be resample, downsample or upsample.
- Frequencies can be specified as strings: "us", "ms", "S", "T", "H", "D", "B", "W", "M", "A", "3min", "2h20", ...
- More aliases at http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

### Exercise

- With `resample` method, convert df Series to 10 year blocks:

+++ {"slideshow": {"slide_type": "slide"}}

### Saving Work

+++ {"slideshow": {"slide_type": "fragment"}}

[HDF5](https://support.hdfgroup.org/HDF5/) is widely used and one of the most powerful file format to store binary data. It allows to store both Series and DataFrames.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
with pd.HDFStore("data/pandas_series.h5") as writer:
    df.to_hdf(writer, "/temperatures/full_globe")
```

+++ {"slideshow": {"slide_type": "slide"}}

### Reloading data

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
with pd.HDFStore("data/pandas_series.h5") as store:
    df = store["/temperatures/full_globe"]
```
