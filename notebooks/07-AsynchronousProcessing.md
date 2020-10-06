---
jupytext:
  cell_metadata_json: true
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: '0.9'
    jupytext_version: 1.5.2
kernelspec:
  display_name: big-data
  language: python
  name: big-data
---

+++ {"slideshow": {"slide_type": "slide"}}

# Asynchronous Processing

While many parallel applications can be described as maps, some can be more complex.
In this section we look at the asynchronous `concurrent.futures` interface,
which provides a simple API for ad-hoc parallelism.
This is useful for when your computations don't fit a regular pattern.

+++ {"slideshow": {"slide_type": "fragment"}}

## Executor.submit

The `submit` method starts a computation in a separate thread or process and immediately gives us a `Future` object that refers to the result.  At first, the future is pending.  Once the function completes the future is finished. 

We collect the result of the task with the `.result()` method,
which does not return until the results are available.

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
%%time
from time import sleep

def slowadd(a, b, delay=1):
    sleep(delay)
    return a + b

slowadd(1,1)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from concurrent.futures import ThreadPoolExecutor

e = ThreadPoolExecutor()
future = e.submit(slowadd, 1, 2)
future
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
future.result()
```

+++ {"slideshow": {"slide_type": "slide"}}

## Submit many tasks, receive many futures

Because submit returns immediately we can submit many tasks all at once and they will execute in parallel.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
results = [slowadd(i, i, delay=1) for i in range(8)]
print(results)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
e = ThreadPoolExecutor()
futures = [e.submit(slowadd, i, i, delay=1) for i in range(8)]
results = [f.result() for f in futures]
print(results)
```

+++ {"slideshow": {"slide_type": "slide"}}

*  Submit fires off a single function call in the background, returning a future.  
*  When we combine submit with a single for loop we recover the functionality of map.  
*  When we want to collect our results we replace each of our futures, `f`, with a call to `f.result()`
*  We can combine submit with multiple for loops and other general programming to get something more general than map.

+++ {"slideshow": {"slide_type": "slide"}}

## Exercise 7.1

Parallelize the following code with e.submit

1.  Replace the `results` list with a list called `futures`
2.  Replace calls to `slowadd` and `slowsub` with `e.submit` calls on those functions
3.  At the end, block on the computation by recreating the `results` list by calling `.result()` on each future in the `futures` list.

+++ {"slideshow": {"slide_type": "slide"}}

## Extract daily stock data from google

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import os  # library to get directory and file paths
import tarfile # this module makes possible to read and write tar archives

def extract_data(name, where):
    datadir = os.path.join(where,name)
    if not os.path.exists(datadir):
       print("Extracting data...")
       tar_path = os.path.join(where, name+'.tgz')
       with tarfile.open(tar_path, mode='r:gz') as data:
          data.extractall(where)
            
extract_data('daily-stock','data') # this function call will extract json files
```

+++ {"slideshow": {"slide_type": "slide"}}

## Convert data to pandas DataFrames and save it in hdf5 files

[HDF5](https://portal.hdfgroup.org/display/support) is a data model, library, and file format for storing and managing data. This format is widely used and is supported by many languages and platforms.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import json
import pandas as pd
import os, glob

here = os.getcwd()
datadir = os.path.join(here,'data','daily-stock')
filenames = sorted(glob.glob(os.path.join(datadir, '*.json')))
filenames
```

+++ {"slideshow": {"slide_type": "slide"}}

### Sequential version

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
from concurrent.futures import ProcessPoolExecutor
import json
import pandas as pd

%rm data/daily-stock/*.h5

def load_parse_store(fn):
    
    with open(fn) as f:
        data = [json.loads(line) for line in f] # load 
        
    df = pd.DataFrame(data) # parse 
    
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data') # store
    print("Finished : %s" % (out_filename.split(os.path.sep)[-1]))
    
    return True

    
results = [load_parse_store(file) for file in filenames]
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 7.2

Parallelize the loop above using `ThreadPoolExecutor` and `map`.

+++ {"slideshow": {"slide_type": "slide"}}

## Read files and load dataframes.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
filenames = sorted(glob.glob(os.path.join('data', 'daily-stock', '*.h5')))
series ={}
for fn in filenames:
    series[fn] = pd.read_hdf(fn)['close']
```

+++ {"slideshow": {"slide_type": "slide"}}

## Application

Given our HDF5 files from the last section we want to find the two datasets with the greatest pair-wise correlation.  This forces us to consider all $n\times(n-1)$ possibilities.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time

results = {}

for a in filenames:
    for b in filenames:
        if a != b:
            results[a, b] = series[a].corr(series[b])
            
((a, b), corr) = max(results.items(), key=lambda kv: kv[1])
print("%s matches with %s with correlation %f" % (a, b, corr))
```

+++ {"slideshow": {"slide_type": "slide"}}

We use matplotlib to visually inspect the highly correlated timeseries

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%matplotlib inline
import matplotlib.pyplot as plt
plt.figure(figsize=(10, 4))
plt.plot(series[a]/series[a].max())
plt.plot(series[b]/series[b].max())
plt.xticks(visible=False);
```

+++ {"slideshow": {"slide_type": "slide"}}

## Analysis

This computation starts out by loading data from disk. We already know how to parallelize it:

```python
series = {}
for fn in filenames:
    series[fn] = pd.read_hdf(fn)['x']
```

It follows with a doubly nested for loop with an if statement.  

```python
results = {}
for a in filenames:
    for b in filenames:
        if a != b:
            results[a, b] = series[a].corr(series[b])
```

It *is* possible to solve this problem with `map`, but it requires some cleverness.  Instead we'll learn `submit`, an interface to start individual function calls asynchronously.

It finishes with a reduction on small data.  This part is fast enough.

```python
((a, b), corr) = max(results.items(), key=lambda kv: kv[1])
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 7.3
- Parallelize pair-wise correlations with `e.submit`
- Implement two versions one using Processes, another with Threads by replacing `e` with a ProcessPoolExecutor:

#### Threads

```python
from concurrent.futures import ThreadPoolExecutor
e = ThreadPoolExecutor(4)
```

#### Processes

Be careful, a `ProcessPoolExecutor` does not run in the jupyter notebook cell.
You must run your file in a terminal.

```python
from concurrent.futures import ProcessPoolExecutor
e = ProcessPoolExecutor(4)
```

- How does performance vary?

+++ {"slideshow": {"slide_type": "slide"}}

Some conclusions about futures
----------------------------

*  `submit` functions can help us to parallelize more complex applications
*  It didn't actually speed up the code very much
*  Threads and Processes give some performance differences
*  This is not very robust.
