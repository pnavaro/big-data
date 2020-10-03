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
import dask-
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
---
slideshow:
  slide_type: slide
---
def read( fn ):
    with open(fn) as f:
        return [json.loads(line) for line in f]
    
def convert(data):
    df = pd.DataFrame(data)
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, os.path.join(here,'data'))
    return

for fn in filenames:  
    data = read( fn)
    convert(data)
    
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
dask_frames = da.stack(lazy_frames[:10], axis=0)  # concatenate arrays along first axis 
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

[Delayed best practices](https://docs.dask.org/en/latest/delayed-best-practices.html)

```{code-cell} ipython3

```
