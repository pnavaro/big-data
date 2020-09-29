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

# Pandas Dataframes

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

pd.set_option("display.max_rows", 8)
plt.rcParams['figure.figsize'] = (9, 6)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Create a [DataFrame](https://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe)

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
dates = pd.date_range('20130101', periods=6)
pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
pd.DataFrame({'A' : 1.,
              'B' : pd.Timestamp('20130102'),
              'C' : pd.Series(1,index=list(range(4)),dtype='float32'),
              'D' : np.arange(4,dtype='int32'),
              'E' : pd.Categorical(["test","train","test","train"]),
              'F' : 'foo' })
```

+++ {"slideshow": {"slide_type": "slide"}}

## Load Data from CSV File

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
url = "https://www.fun-mooc.fr/c4x/agrocampusouest/40001S03/asset/AnaDo_JeuDonnees_TemperatFrance.csv"
french_cities = pd.read_csv(url, delimiter=";", encoding="latin1", index_col=0)
french_cities
```

+++ {"slideshow": {"slide_type": "slide"}}

## Viewing Data

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities.head()
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
french_cities.tail()
```

+++ {"slideshow": {"slide_type": "slide"}}

## Index

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities.index
```

+++ {"slideshow": {"slide_type": "fragment"}}

We can rename an index by setting its name.

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
french_cities.index.name = "City"
french_cities.head()
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise: Rename DataFrame Months in English

+++ {"slideshow": {"slide_type": "slide"}}

## From a local or remote HTML file
We can download and extract data about mean sea level stations around the world from the [PSMSL website](http://www.psmsl.org/).

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
# Needs `lxml`, `beautifulSoup4` and `html5lib` python packages
table_list = pd.read_html("http://www.psmsl.org/data/obtaining/")
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
# there is 1 table on that page which contains metadata about the stations where 
# sea levels are recorded
local_sea_level_stations = table_list[0]
local_sea_level_stations
```

+++ {"slideshow": {"slide_type": "slide"}}

## Indexing on DataFrames

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities['Lati']  # DF [] accesses columns (Series)
```

+++ {"slideshow": {"slide_type": "slide"}}

`.loc` and `.iloc` allow to access individual values, slices or masked selections:

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities.loc['Rennes', "Sep"]
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities.loc['Rennes', ["Sep", "Dec"]]
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities.loc['Rennes', "Sep":"Dec"]
```

+++ {"slideshow": {"slide_type": "slide"}}

## Masking

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
mask = [True, False] * 6 + 5 * [False]
print(french_cities.iloc[:, mask])
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
print(french_cities.loc["Rennes", mask])
```

+++ {"slideshow": {"slide_type": "slide"}}

## New column

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities["std"] = french_cities.iloc[:,:12].std(axis=1)
french_cities
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities = french_cities.drop("std", axis=1) # remove this new column
```

```{code-cell} ipython3
---
slideshow:
  slide_type: skip
---
french_cities
```

+++ {"slideshow": {"slide_type": "slide"}}

## Modifying a dataframe with multiple indexing

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
# french_cities['Rennes']['Sep'] = 25 # It does not works and breaks the DataFrame
french_cities.loc['Rennes']['Sep'] # = 25 is the right way to do it
```

```{code-cell} ipython3
---
slideshow:
  slide_type: skip
---
french_cities
```

+++ {"slideshow": {"slide_type": "slide"}}

## Transforming datasets

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities['Mean'].min(), french_cities['Ampl'].max()
```

+++ {"slideshow": {"slide_type": "fragment"}}

## Apply

Let's convert the temperature mean from Celsius to Fahrenheit degree.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
fahrenheit = lambda T: T*9/5+32
french_cities['Mean'].apply(fahrenheit)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Sort

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities.sort_values(by='Lati')
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
french_cities = french_cities.sort_values(by='Lati',ascending=False)
french_cities
```

+++ {"slideshow": {"slide_type": "slide"}}

## Stack and unstack

Instead of seeing the months along the axis 1, and the cities along the axis 0, let's try to convert these into an outer and an inner axis along only 1 time dimension.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
pd.set_option("display.max_rows", 20)
unstacked = french_cities.iloc[:,:12].unstack()
unstacked
```

```{code-cell} ipython3
---
slideshow:
  slide_type: subslide
---
type(unstacked)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Transpose

+++ {"slideshow": {"slide_type": "fragment"}}

The result is grouped in the wrong order since it sorts first the axis that was unstacked. We need to transpose the dataframe.

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
city_temp = french_cities.iloc[:,:12].transpose()
city_temp.plot()
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
city_temp.boxplot(rot=90);
```

+++ {"slideshow": {"slide_type": "slide"}}

## Describing

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities['Région'].describe()
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities['Région'].unique()
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
french_cities['Région'].value_counts()
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
# To save memory, we can convert it to a categorical column:
french_cities["Région"] = french_cities["Région"].astype("category")
```

```{code-cell} ipython3
---
slideshow:
  slide_type: skip
---
french_cities.memory_usage()
```

+++ {"slideshow": {"slide_type": "slide"}}

## Data Aggregation/summarization

## groupby

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
fc_grouped_region = french_cities.groupby("Région")
type(fc_grouped_region)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
for group_name, subdf in fc_grouped_region:
    print(group_name)
    print(subdf)
    print("")
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise

Consider the following dataset [UCI Machine Learning Repository Combined Cycle Power Plant Data Set](https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant).
This dataset consists of records of measurements relating to peaker power plants of 10000 points over 6 years (2006-2011).

**Variables**
- AT = Atmospheric Temperature in C
- V = Exhaust Vaccum Speed
- AP = Atmospheric Pressure
- RH = Relative Humidity
- PE = Power Output

We want to model the power output as a function of the other parameters.

Observations are in 5 excel sheets of about 10000 records in "Folds5x2_pp.xlsx". These 5 sheets are same data shuffled.
- Read this file with the pandas function [read_excel](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html). What is the type returned by this function?
- Implement a `select` function to regroup all observations in a pandas serie.
- Use `select` function and `corr` to compute the maximum correlation.
- Parallelize this loop with `concurrent.futures`. 

```{code-cell} ipython3

```
