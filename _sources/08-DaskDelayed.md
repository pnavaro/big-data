---
jupytext:
  cell_metadata_json: true
  formats: ipynb,md:myst
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

# Dask

<img src="images/dask_logo.jpg">

+++ {"slideshow": {"slide_type": "slide"}}

- process data that doesn't fit into memory by breaking it into blocks and specifying task chains
- parallelize execution of tasks across cores and even nodes of a cluster
- move computation to the data rather than the other way around, to minimize communication overheads

http://dask.pydata.org/en/latest/

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import dask
import dask.multiprocessing
```

+++ {"slideshow": {"slide_type": "slide"}}

## Define two slow functions

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from time import sleep

def slowinc(x, delay=1):
    sleep(delay)
    return x + 1

def slowadd(x, y, delay=1):
    sleep(delay)
    return x + y
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
x = slowinc(1)
y = slowinc(2)
z = slowadd(x, y)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Parallelize with dask.delayed

- Functions wrapped by `dask.delayed` don't run immediately, but instead put those functions and arguments into a task graph. 
- The result is computed separately by calling the `.compute()` method.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from dask import delayed
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
x = delayed(slowinc)(1)
y = delayed(slowinc)(2)
z = delayed(slowadd)(x, y)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
z.compute()
```

+++ {"slideshow": {"slide_type": "slide"}}

## Dask graph

- Contains description of the calculations necessary to produce the result. 
- The z object is a lazy Delayed object. This object holds everything we need to compute the final result. We can compute the result with .compute() as above or we can visualize the task graph for this value with .visualize().

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
z.visualize()
```

+++ {"slideshow": {"slide_type": "slide"}}

## Parallelize a loop

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
data = list(range(8))

results = []

for x in data:
    y = slowinc(x)
    results.append(y)

total = sum(results)
total
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 8.1

- Parallelize this by appending the delayed `slowinc` calls to the list `results`.
- Display the graph of `total` computation
- Compute time elapsed for the computation.

+++ {"slideshow": {"slide_type": "slide"}}

## Decorator

It is also common to see the delayed function used as a decorator. Same example:

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time

@dask.delayed
def slowinc(x, delay=1):
    sleep(delay)
    return x + 1

@dask.delayed
def slowadd(x, y, delay=1):
    sleep(delay)
    return x + y

x = slowinc(1)
y = slowinc(2)
z = slowadd(x, y)
z.compute()
```

+++ {"slideshow": {"slide_type": "slide"}}

## Control flow
-  Delay only some functions, running a few of them immediately. This is helpful when those functions are fast and help us to determine what other slower functions we should call. 
- In the example below we iterate through a list of inputs. If that input is even then we want to call `half`. If the input is odd then we want to call `odd_process`. This iseven decision to call `half` or `odd_process` has to be made immediately (not lazily) in order for our graph-building Python code to proceed.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from random import randint
import dask.delayed

def half(x):
    sleep(1)
    return x // 2

def odd_process(x):
    sleep(1)
    return 3*x+1

def is_even(x):
    return not x % 2

data = [randint(0,100) for i in range(8)]
data
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 8.2
- Parallelize the sequential code above using dask.delayed
- You will need to delay some functions, but not all
- Visualize and check the computed result

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 8.3
- Parallelize the hdf5 conversion from json files
- Create a function `convert_to_hdf`
- Use dask.compute function on delayed calls of the funtion created list
- Is it really  faster as expected ?

Hint: Read [Delayed Best Practices](http://dask.pydata.org/en/latest/delayed-best-practices.html)

```{code-cell} ipython3
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

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import os, sys
from glob import glob
import pandas as pd
import json

here = os.getcwd() # get the current directory
filenames = sorted(glob(os.path.join(here,'data', 'daily-stock', '*.json')))
```

```{code-cell} ipython3
%%time
def read( fn ):
    " read json file "
    with open(fn) as f:
        return [json.loads(line) for line in f]
    
def convert(data, fn):
    " convert json file to hdf5 file"
    df = pd.DataFrame(data)
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')
    return fn[:-5]
 
results = []
for filename in filenames:
    data = read(filename)
    results.append(convert(data, filename))
 
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
@dask.delayed
def read( fn ):
    " read json file "
    with open(fn) as f:
        return [json.loads(line) for line in f]
    
@dask.delayed
def convert(data, fn):
    " convert json file to hdf5 file"
    df = pd.DataFrame(data)
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')
    return fn[:-5]
 
results = []
for filename in filenames:
    data = read(filename)
    results.append(convert(data, filename))
 
%time dask.compute(*results)
```

```{code-cell} ipython3
%ls data/daily-stock/*.h5
```

## Read multiple files with Dask Arrays

```{code-cell} ipython3
from tqdm import tqdm
from PIL import Image
import dask
import dask.delayed as delayed
import dask.array as da
from glob import glob
import h5py as h5
import numpy as np
```

In this dataset we have two dimensional field records along time. Every h5 file contains a matrix.

**Don't download the files if you work on svmass2 server, you can find them in** `datasets/fvalues`.

+++

Download the file from [https://github.com/MMASSD/datasets](https://github.com/MMASSD/datasets/blob/master/fvalues.tgz)

+++

```bash
wget https://github.com/MMASSD/datasets/raw/master/fvalues.tgz
```

+++

This file is a zip archive we need to uncompress and extract.

```{code-cell} ipython3
extract_data('fvalues','.') 
```

You get 1000 h5 files

```{code-cell} ipython3
filenames = sorted(glob("*.h5"))
filenames[:5], len(filenames)
```

In order to plot these fields, we will scale them between 0 to 255 grey levels.

```{code-cell} ipython3
import numpy as np
def scale(x) :
    "Scale field to 0-255 levels"
    return np.uint8(255*(x-np.min(x)) / (np.max(x)-np.min(x)))
```

Let's create a function that read the file and return the scaled field.

```{code-cell} ipython3
import h5py as h5
def read_frame( filepath ):
    " Create image from the `dataset` of h5 file `filepath` "
    with h5.File(filepath, "r") as f:
        z = f.get("values")
        return scale(z)
```

```{code-cell} ipython3
image = read_frame( filenames[0])
image.shape
```

```{code-cell} ipython3
from PIL import Image 
Image.fromarray(image)
```

With NumPy we might allocate a big array and then iteratively load images and place them into this array `serial_frames`.

```{code-cell} ipython3
%%time
serial_frames = np.empty((1000,*image.shape), dtype=np.uint8)
for i, fn in enumerate(filenames):
    serial_frames[i, :, :] = read_frame(fn)
```

```{code-cell} ipython3
from ipywidgets import interact, IntSlider
 
def display_sequence(iframe):
     return Image.fromarray(serial_frames[iframe,:,:])
     
interact(display_sequence, 
          iframe=IntSlider(min=0,
                           max=np.shape(serial_frames)[0]-1,
                           step=1,
                           value=0, 
                           continuous_update=True));
```

In the code above, we read all images and store them in memory. If you have more plots or bigger images it won't fit in your computer memory. You have two options:
- Use a bigger computer
- Not store all files in memory and read only the file that contains the field you want to display.

+++

## Use dask to read and display images

We can delayed the read function

```{code-cell} ipython3
lazy_read = delayed(read_frame)
lazy_frames = [lazy_read(fn) for fn in filenames]
```

Instead of `serial_frames`, we create an array of delayed tasks.

```{code-cell} ipython3
import dask.array as da
lazy_frames = [da.from_delayed(lazy_read,# Construct a small Dask array
                           dtype=image.dtype,   # for every lazy value
                           shape=image.shape)
          for lazy_read in lazy_frames]
lazy_frames[0]
```

```{code-cell} ipython3
dask_frames = da.stack(lazy_frames, axis=0)  # concatenate arrays along first axis 
```

```{code-cell} ipython3
dask_frames 
```

```{code-cell} ipython3
dask_frames = dask_frames.rechunk((10, 257, 257))   
dask_frames
```

```{code-cell} ipython3
Image.fromarray(scale(dask_frames.sum(axis=0).compute()))
```

```{code-cell} ipython3

```

```{code-cell} ipython3
from ipywidgets import interact, IntSlider

def display_sequence(iframe):
    
    return Image.fromarray(dask_frames[iframe,:,:].compute())
    
interact(display_sequence, 
         iframe=IntSlider(min=0,
                          max=len(dask_frames)-1,
                          step=1,
                          value=0, 
                          continuous_update=True))
```

Everytime you move the slider, it will read the corresponding file and load the frame. That's why you need to wait a little to get your image. You load image one by one and you can handle a very large amount of images.

+++

### Some questions to consider:

-  Why did we go from 3s to 2s?  Why weren't we able to parallelize down to 1s?
-  What would have happened if the inc and add functions didn't include the `sleep(1)`?  Would Dask still be able to speed up this code?
-  What if we have multiple outputs or also want to get access to x or y?

+++

## Exercise: Parallelizing a Pandas Groupby Reduction

In this exercise we read several CSV files and perform a groupby operation in parallel.  We are given sequential code to do this and parallelize it with `dask.delayed`.

The computation we will parallelize is to compute the mean departure delay per airport from some historical flight data.  We will do this by using `dask.delayed` together with `pandas`.  In a future section we will do this same exercise with `dask.dataframe`.

+++

### Prep data

First, run this code to prep some data.  You don't need to understand this code.

This extracts some historical flight data for flights out of NYC between 1990 and 2000. The data is taken from [here](http://stat-computing.org/dataexpo/2009/the-data.html). This should only take a few seconds to run.

+++

### Inspect data

Data are in the file `data/nycflights.tar.gz`. You can extract them with the command
```bash
tar zxvf nycflights.tar.gz
```
According to your operating system, double click on the file could do the job.

```{code-cell} ipython3
extract_data('nycflights','data')
```

```{code-cell} ipython3
import os
sorted(os.listdir(os.path.join('data', 'nycflights')))
```

### Read one file with `pandas.read_csv` and compute mean departure delay

```{code-cell} ipython3
import pandas as pd
df = pd.read_csv(os.path.join('data', 'nycflights', '1990.csv'))
df.head()
```

```{code-cell} ipython3
# What is the schema?
df.dtypes
```

```{code-cell} ipython3
# What originating airports are in the data?
df.Origin.unique()
```

```{code-cell} ipython3
# Mean departure delay per-airport for one year
df.groupby('Origin').DepDelay.mean()
```

### Sequential code: Mean Departure Delay Per Airport

The above cell computes the mean departure delay per-airport for one year. Here we expand that to all years using a sequential for loop.

```{code-cell} ipython3
from glob import glob
filenames = sorted(glob(os.path.join('data', 'nycflights', '*.csv')))
```

```{code-cell} ipython3
%%time

sums = []
counts = []
for fn in filenames:
    # Read in file
    df = pd.read_csv(fn)
    
    # Groupby origin airport
    by_origin = df.groupby('Origin')
    
    # Sum of all departure delays by origin
    total = by_origin.DepDelay.sum()
    
    # Number of flights by origin
    count = by_origin.DepDelay.count()
    
    # Save the intermediates
    sums.append(total)
    counts.append(count)

# Combine intermediates to get total mean-delay-per-origin
total_delays = sum(sums)
n_flights = sum(counts)
mean = total_delays / n_flights
```

```{code-cell} ipython3
mean
```

### Exercise : Parallelize the code above

Use `dask.delayed` to parallelize the code above.  Some extra things you will need to know.

1.  Methods and attribute access on delayed objects work automatically, so if you have a delayed object you can perform normal arithmetic, slicing, and method calls on it and it will produce the correct delayed calls.

    ```python
    x = delayed(np.arange)(10)
    y = (x + 1)[::2].sum()  # everything here was delayed
    ```
2.  Calling the `.compute()` method works well when you have a single output.  When you have multiple outputs you might want to use the `dask.compute` function:

    ```python
    >>> x = delayed(np.arange)(10)
    >>> y = x ** 2
    >>> min, max = compute(y.min(), y.max())
    (0, 81)
    ```
    
    This way Dask can share the intermediate values (like `y = x**2`)
    
So your goal is to parallelize the code above (which has been copied below) using `dask.delayed`.  You may also want to visualize a bit of the computation to see if you're doing it correctly.

+++

[Delayed best practices](https://docs.dask.org/en/latest/delayed-best-practices.html)
