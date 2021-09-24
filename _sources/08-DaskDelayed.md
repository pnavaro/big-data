---
jupytext:
  cell_metadata_json: true
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.11.5
kernelspec:
  display_name: big-data
  language: python
  name: big-data
---

+++ {"cell_id": "00000-52ebd828-d2aa-4150-94c4-457471b6a3cd", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

# Dask

<img src="images/dask_logo.jpg">

+++ {"cell_id": "00001-79be7837-20f4-4102-85cd-a731ae9f3626", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

- process data that doesn't fit into memory by breaking it into blocks and specifying task chains
- parallelize execution of tasks across cores and even nodes of a cluster
- move computation to the data rather than the other way around, to minimize communication overheads

http://dask.pydata.org/en/latest/

```{code-cell} ipython3
---
cell_id: 00002-4d25f43d-8a30-42d0-b4a8-406689ce13cf
deepnote_cell_type: code
execution_millis: 261
execution_start: 1604928157843
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: 145bccde
---
import dask
import dask.multiprocessing
```

+++ {"cell_id": "00003-b50ba8be-ea79-4407-a128-e8737321f008", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

## Define two slow functions

```{code-cell} ipython3
---
cell_id: 00004-bdaf2b7c-2b02-488d-9dca-89cf289d6636
deepnote_cell_type: code
execution_millis: 1
execution_start: 1604928159401
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: 4246c45a
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
cell_id: 00005-709c6ad2-c285-41cc-9242-2731f719b06c
deepnote_cell_type: code
execution_millis: 3004
execution_start: 1604928160391
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: 5a5a4ebf
---
%%time
x = slowinc(1)
y = slowinc(2)
z = slowadd(x, y)
```

+++ {"cell_id": "00006-202d629c-5b8f-454f-9bbc-d20eae140d8d", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

## Parallelize with dask.delayed

- Functions wrapped by `dask.delayed` don't run immediately, but instead put those functions and arguments into a task graph. 
- The result is computed separately by calling the `.compute()` method.

```{code-cell} ipython3
---
cell_id: 00007-b872f4aa-b348-44cd-9add-a6947c947632
deepnote_cell_type: code
execution_millis: 2
execution_start: 1604928168699
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: 6ebfca2f
---
from dask import delayed
```

```{code-cell} ipython3
---
cell_id: 00008-50c7cc67-ef95-4abc-a436-5d2383a61263
deepnote_cell_type: code
execution_millis: 4
execution_start: 1604928169627
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: bd3eeed4
---
x = dask.delayed(slowinc)(1)
y = dask.delayed(slowinc)(2)
z = dask.delayed(slowadd)(x, y)
```

```{code-cell} ipython3
---
cell_id: 00009-23d17b22-0a3d-4e32-8aae-08426be7c50b
deepnote_cell_type: code
execution_millis: 2047
execution_start: 1604928170971
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: 8d6c6701
---
%%time
z.compute()
```

+++ {"cell_id": "00010-3be0ba87-44a8-4869-b685-8098d9c02b96", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

## Dask graph

- Contains description of the calculations necessary to produce the result. 
- The z object is a lazy Delayed object. This object holds everything we need to compute the final result. We can compute the result with .compute() as above or we can visualize the task graph for this value with .visualize().

```{code-cell} ipython3
---
cell_id: 00011-5f32f2e0-51f9-4e56-9ea1-6a58c815c6c5
deepnote_cell_type: code
execution_millis: 114
execution_start: 1604928174844
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: 3d8a266a
---
z.visualize()
```

+++ {"cell_id": "00012-a85c61c5-ba60-412b-88ed-c2ced106d49b", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

## Parallelize a loop

```{code-cell} ipython3
---
cell_id: 00013-c881baab-40c3-47d6-b513-321ffe5a8588
deepnote_cell_type: code
execution_millis: 15
execution_start: 1604928177369
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: ca27d9b6
---
%%time
data = list(range(8))

tasks = []

for x in data:
    y = delayed(slowinc)(x)
    tasks.append(y)

total = delayed(sum)(tasks)
total
```

```{code-cell} ipython3
:cell_id: 00017-6ad2a2a3-f126-470d-9c2d-ffdbc4d69499
:deepnote_cell_type: code
:execution_millis: 131
:execution_start: 1604928178279
:output_cleared: false
:source_hash: c8188fb7
:tags: []

total.visualize()
```

```{code-cell} ipython3
:cell_id: 00016-4a04c353-8948-4c66-9fb5-e437df06de1c
:deepnote_cell_type: code
:execution_millis: 4030
:execution_start: 1604928179742
:output_cleared: false
:source_hash: 9af19944
:tags: []

total.compute()
```

+++ {"cell_id": "00014-4ee97017-a512-490a-9dab-3930e7ac5d72", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

### Exercise 8.1

- Parallelize this by appending the delayed `slowinc` calls to the list `results`.
- Display the graph of `total` computation
- Compute time elapsed for the computation.

+++ {"cell_id": "00015-1437e432-172c-4d08-86c1-b299ba80a3cf", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

## Decorator

It is also common to see the delayed function used as a decorator. Same example:

```{code-cell} ipython3
---
cell_id: 00016-45f52943-f239-4265-b026-8110f2c9c1a7
deepnote_cell_type: code
execution_millis: 2017
execution_start: 1604928185931
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: 52b83e58
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

```{code-cell} ipython3
:cell_id: 00019-e4fb6083-ef12-4369-a226-334da8875343
:deepnote_cell_type: code
:execution_millis: 83
:execution_start: 1604928187955
:output_cleared: false
:source_hash: 3d8a266a
:tags: []

z.visualize()
```

+++ {"cell_id": "00017-0d2d6be9-893a-43d2-9369-cb6ef8c9e8ae", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

## Control flow
-  Delay only some functions, running a few of them immediately. This is helpful when those functions are fast and help us to determine what other slower functions we should call. 
- In the example below we iterate through a list of inputs. If that input is even then we want to call `half`. If the input is odd then we want to call `odd_process`. This iseven decision to call `half` or `odd_process` has to be made immediately (not lazily) in order for our graph-building Python code to proceed.

```{code-cell} ipython3
---
cell_id: 00018-7a4c86ba-18df-432a-bd2a-1b767dc6b10f
deepnote_cell_type: code
execution_millis: 1
execution_start: 1604928190542
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: c1388939
---
from random import randint
import dask.delayed

@delayed
def half(x):
    sleep(1)
    return x // 2

@delayed
def odd_process(x):
    sleep(1)
    return 3*x+1

def is_even(x):
    return not x % 2

data = [randint(0,100) for i in range(8)]

result = []
for x in data:
    if is_even(x):
        result.append(half(x))
    else:
        result.append(odd_process(x))

total = delayed(sum)(result)
```

```{code-cell} ipython3
:cell_id: 00024-86fb0e82-440e-40c3-b896-fdd6975a1794
:deepnote_cell_type: code
:execution_millis: 139
:execution_start: 1604928191593
:output_cleared: false
:source_hash: c8188fb7
:tags: []

total.visualize()
```

```{code-cell} ipython3
:cell_id: 00025-475c8ac6-c7f1-45de-ad7b-85e8746b0bea
:deepnote_cell_type: code
:execution_millis: 3973
:execution_start: 1604928196027
:output_cleared: false
:source_hash: 9af19944
:tags: []

total.compute()
```

+++ {"cell_id": "00019-54a20f04-7b92-4998-a411-029fe228b7d8", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

### Exercise 8.2
- Parallelize the sequential code above using dask.delayed
- You will need to delay some functions, but not all
- Visualize and check the computed result

+++ {"cell_id": "00020-46e8c1d4-c7ca-4f16-b6ed-76d60e6b2a7d", "deepnote_cell_type": "markdown", "output_cleared": false, "slideshow": {"slide_type": "slide"}}

### Exercise 8.3
- Parallelize the hdf5 conversion from json files
- Create a function `convert_to_hdf`
- Use dask.compute function on delayed calls of the funtion created list
- Is it really  faster as expected ?

Hint: Read [Delayed Best Practices](http://dask.pydata.org/en/latest/delayed-best-practices.html)

```{code-cell} ipython3
:cell_id: 00021-2eebfde2-13e4-4f69-bb93-d11e0216112e
:deepnote_cell_type: code
:execution_millis: 7
:execution_start: 1604928202619
:output_cleared: false
:source_hash: cae726ac

import os  # library to get directory and file paths
import tarfile # this module makes possible to read and write tar archives

def extract_data(name, where):
    datadir = os.path.join(where) # directory where extract all datafile
    if not os.path.exists(datadir): # check if this directory exists
       print("Extracting data...")
       tar_path = os.path.join(name+'.tgz')  # path to the tgz file
       with tarfile.open(tar_path, mode='r:gz') as data: # open the tgz file
          data.extractall(datadir)  # extract all data file in datadir
            
extract_data('daily-stock','data') # this function call will extract json files
```

```{code-cell} ipython3
---
cell_id: 00022-0c5ae210-e929-4dc9-80a4-65d82633f166
deepnote_cell_type: code
execution_millis: 1
execution_start: 1604928207401
output_cleared: false
slideshow:
  slide_type: fragment
source_hash: c6c7aed9
---
import dask
import os, sys
from glob import glob
import pandas as pd
import json

here = os.getcwd() # get the current directory
filenames = sorted(glob(os.path.join(here,'data', 'daily-stock', '*.json')))
```

```{code-cell} ipython3
:cell_id: 00030-8e4a6346-7542-46f8-908c-656950c3e738
:deepnote_cell_type: code
:execution_millis: 1
:execution_start: 1604928209989
:output_cleared: false
:source_hash: 72b32632
:tags: []

filenames[:5]
```

```{code-cell} ipython3
:cell_id: 00032-468f2ab1-6937-4414-bc93-3e503beff036
:deepnote_cell_type: code
:execution_millis: 1272
:execution_start: 1604928213763
:output_cleared: false
:source_hash: 16bc38b6
:tags: []

%rm data/daily-stock/*.h5
```

+++ {"cell_id": "00025-1d678b08-595c-41ed-8532-ac9e1fdb281e", "deepnote_cell_type": "markdown", "output_cleared": false}

## Read multiple files with Dask Arrays

```{code-cell} ipython3
:cell_id: 00026-d7df423f-4e01-4f9a-91ed-f2cd3145dd6d
:deepnote_cell_type: code
:execution_millis: 336
:execution_start: 1604928252896
:output_cleared: false
:source_hash: 7afa2800

from tqdm import tqdm # barre de progression
from PIL import Image # AFFICHER DES IMAGES DEPUIS UN TABLEAU 2D
import dask 
import dask.delayed as delayed
import dask.array as da
from glob import glob  # Lister des fichiers
import h5py as h5 # ecrire et lire des fichiers au format hdf5
import numpy as np # numpy pour normaliser les images
```

+++ {"cell_id": "00027-caa6742e-303a-4d82-a07c-48ec3f63ac5e", "deepnote_cell_type": "markdown", "output_cleared": false}

In this dataset we have two dimensional field records along time. Every h5 file contains a matrix.

+++ {"cell_id": "00028-57461c4e-7579-4a69-8999-42f30ecc7bf1", "deepnote_cell_type": "markdown", "output_cleared": false}

Data are already downloaded in datasets directory. You can download the file from [https://github.com/MMASSD/datasets](https://github.com/MMASSD/datasets/blob/master/fvalues.tgz)

+++ {"cell_id": "00029-3b876110-e8fd-438a-a64f-959c3c319124", "deepnote_cell_type": "markdown", "output_cleared": false}

!wget https://github.com/MMASSD/datasets/raw/master/fvalues.tgz

+++ {"cell_id": "00030-26c29e37-dd16-4f20-a79b-ca0458177384", "deepnote_cell_type": "markdown", "output_cleared": false}

This file is a zip archive we need to uncompress and extract.

```{code-cell} ipython3
:cell_id: 00031-caab7c6a-4b86-498b-99b0-b9984eaa4e79
:deepnote_cell_type: code
:execution_millis: 25239
:output_cleared: false
:source_hash: 784e4ddc

# extract_data('fvalues','.') 
```

+++ {"cell_id": "00032-65bc1f33-f358-460e-88e9-dce251a84c9c", "deepnote_cell_type": "markdown", "output_cleared": false}

You get 1000 h5 files

```{code-cell} ipython3
:cell_id: 00033-89147e3c-0b69-4328-a745-4e0c24b374c1
:deepnote_cell_type: code
:execution_millis: 5
:execution_start: 1604928267075
:output_cleared: false
:source_hash: 65560be

filenames = sorted(glob("fvalues/*.h5"))
filenames[:5], len(filenames)
```

+++ {"cell_id": "00034-e9e14838-989a-43f6-806d-819a50a947cd", "deepnote_cell_type": "markdown", "output_cleared": false}

In order to plot these fields, we will scale them between 0 to 255 grey levels.

```{code-cell} ipython3
:cell_id: 00035-affdedb4-9cd0-4769-b036-144de5e0a875
:deepnote_cell_type: code
:execution_millis: 4
:execution_start: 1604928273895
:output_cleared: false
:source_hash: 7cfa0d5e

import numpy as np
def scale(x) :
    "Scale field to 0-255 levels"
    return np.uint8(255*(x-np.min(x)) / (np.max(x)-np.min(x)))
```

+++ {"cell_id": "00036-c7569ad7-84e4-4561-bf44-8c5518faf9aa", "deepnote_cell_type": "markdown", "output_cleared": false}

Let's create a function that read the file and return the scaled field.

```{code-cell} ipython3
:cell_id: 00037-89107980-3f98-49db-91b1-1fb4c80facb4
:deepnote_cell_type: code
:execution_millis: 7
:execution_start: 1604928275090
:output_cleared: false
:source_hash: b6356dd3

import h5py as h5
def read_frame( filepath ):
    " Create image from the `dataset` of h5 file `filepath` "
    with h5.File(filepath, "r") as f:
        z = f.get("values")
        return scale(z)
```

```{code-cell} ipython3
:cell_id: 00038-506a2dd1-e6b0-4376-b864-20ce78f4b61e
:deepnote_cell_type: code
:execution_millis: 24
:execution_start: 1604928276238
:output_cleared: false
:source_hash: 13b34650

image = read_frame( filenames[0])
image.shape
```

```{code-cell} ipython3
:cell_id: 00039-9bbfa08a-a867-4984-88bc-e1f818277d61
:deepnote_cell_type: code
:execution_millis: 22
:execution_start: 1604928277545
:output_cleared: false
:source_hash: 1570322a

from PIL import Image 
Image.fromarray(image)
```

+++ {"cell_id": "00040-83050c17-db1d-4d5e-b555-ed8edba36074", "deepnote_cell_type": "markdown", "output_cleared": false}

With NumPy we might allocate a big array and then iteratively load images and place them into this array `serial_frames`.

```{code-cell} ipython3
:cell_id: 00041-6ed3bbbc-5c62-46bc-b81b-a9bb29b28023
:deepnote_cell_type: code
:execution_millis: 11638
:execution_start: 1604928280939
:output_cleared: false
:source_hash: b4f7c1d1

%%time
serial_frames = np.empty((1000,*image.shape), dtype=np.uint8)
for i, fn in enumerate(filenames):
    serial_frames[i, :, :] = read_frame(fn)
```

```{code-cell} ipython3
:cell_id: 00042-8e4ee63d-6cc6-41d6-beef-bf871befa6c3
:deepnote_cell_type: code
:execution_millis: 156
:execution_start: 1604928294887
:output_cleared: false
:source_hash: 264bfe9e

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

+++ {"cell_id": "00043-cf51f8eb-6df8-407e-ba2f-0cb3dcb7616e", "deepnote_cell_type": "markdown", "output_cleared": false}

In the code above, we read all images and store them in memory. If you have more plots or bigger images it won't fit in your computer memory. You have two options:
- Use a bigger computer
- Not store all files in memory and read only the file that contains the field you want to display.

+++ {"cell_id": "00044-baa5fd6e-59e8-4835-ab51-945dca4ca13a", "deepnote_cell_type": "markdown", "output_cleared": false}

## Use dask to read and display images

We can delayed the read function

```{code-cell} ipython3
:cell_id: 00045-a926ce03-7f3e-43ad-972f-cfee08468ab3
:deepnote_cell_type: code
:execution_millis: 11
:execution_start: 1604928303537
:output_cleared: false
:source_hash: 2111e2c5

lazy_read = delayed(read_frame)
lazy_frames = [lazy_read(fn) for fn in filenames]
```

+++ {"cell_id": "00046-1af26ad9-8a61-4c0b-b58e-b1885f147051", "deepnote_cell_type": "markdown", "output_cleared": false}

Instead of `serial_frames`, we create an array of delayed tasks.

```{code-cell} ipython3
:cell_id: 00047-01e1112c-5e50-4c51-b70d-a28ebf0b0dec
:deepnote_cell_type: code
:execution_millis: 342
:execution_start: 1604928306371
:output_cleared: false
:source_hash: 63f55fd3

import dask.array as da
lazy_frames = [da.from_delayed(lazy_read,# Construct a small Dask array
                           dtype=image.dtype,   # for every lazy value
                           shape=image.shape)
          for lazy_read in lazy_frames]
lazy_frames[0]
```

```{code-cell} ipython3
:cell_id: 00048-ff157d72-3369-4bf2-ab95-61422862c2b0
:deepnote_cell_type: code
:execution_millis: 6
:execution_start: 1604928308267
:output_cleared: false
:source_hash: 549cb9ec

dask_frames = da.stack(lazy_frames[:100], axis=0)  # concatenate arrays along first axis 
```

```{code-cell} ipython3
:cell_id: 00049-01b8685f-cbdd-4ed0-81fb-ea5ca01bc342
:deepnote_cell_type: code
:execution_millis: 12
:execution_start: 1604928309488
:output_cleared: false
:source_hash: db88a505

dask_frames 
```

```{code-cell} ipython3
:cell_id: 00050-d71a862e-dcbe-477b-9ce2-cbc372ebf2ec
:deepnote_cell_type: code
:execution_millis: 8
:execution_start: 1604928311662
:output_cleared: false
:source_hash: b35eed0b

dask_frames = dask_frames.rechunk((10, 257, 257))   
dask_frames
```

```{code-cell} ipython3
:cell_id: 00051-797f2547-7e78-45f7-a1e0-d82941866917
:deepnote_cell_type: code
:execution_millis: 1533
:execution_start: 1604928312412
:output_cleared: false
:source_hash: fd038353

Image.fromarray(scale(dask_frames.mean(axis=0).compute()))
```

```{code-cell} ipython3
:cell_id: 00052-f989deee-56d3-4d68-8289-4049ea4e5df9
:deepnote_cell_type: code
:execution_millis: 174
:execution_start: 1604928313981
:output_cleared: false
:source_hash: fea8be9f

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

+++ {"cell_id": "00053-11bdbfc8-e67f-4a02-bec0-24c81d8dccbe", "deepnote_cell_type": "markdown", "output_cleared": false}

Everytime you move the slider, it will read the corresponding file and load the frame. That's why you need to wait a little to get your image. You load image one by one and you can handle a very large amount of images.

+++ {"cell_id": "00056-29f2316c-6e07-42ba-a052-0e64beb83831", "deepnote_cell_type": "markdown", "output_cleared": false}

### Some questions to consider:

-  Why did we go from 3s to 2s?  Why weren't we able to parallelize down to 1s?
-  What would have happened if the inc and add functions didn't include the `sleep(1)`?  Would Dask still be able to speed up this code?
-  What if we have multiple outputs or also want to get access to x or y?

+++ {"cell_id": "00057-db7bc273-1a6b-48c5-bf47-9b0817c22b62", "deepnote_cell_type": "markdown", "output_cleared": false}

## Exercise: Parallelizing a Pandas Groupby Reduction

In this exercise we read several CSV files and perform a groupby operation in parallel.  We are given sequential code to do this and parallelize it with `dask.delayed`.

The computation we will parallelize is to compute the mean departure delay per airport from some historical flight data.  We will do this by using `dask.delayed` together with `pandas`.  In a future section we will do this same exercise with `dask.dataframe`.

+++ {"cell_id": "00058-130d2bda-07f2-4e27-93b4-58f8b850345f", "deepnote_cell_type": "markdown", "output_cleared": false}

### Prep data

First, run this code to prep some data.  You don't need to understand this code.

This extracts some historical flight data for flights out of NYC between 1990 and 2000. The data is taken from [here](http://stat-computing.org/dataexpo/2009/the-data.html). This should only take a few seconds to run.

+++ {"cell_id": "00059-4f7a8292-507b-4690-8e34-de2b8a5aac60", "deepnote_cell_type": "markdown", "output_cleared": false}

### Inspect data

Data are in the file `data/nycflights.tar.gz`. You can extract them with the command
```bash
tar zxvf nycflights.tar.gz
```
According to your operating system, double click on the file could do the job.

```{code-cell} ipython3
:cell_id: 00060-3ed75ae6-57a0-4eed-9e09-30012046e27d
:deepnote_cell_type: code
:execution_millis: 3
:execution_start: 1604928324013
:output_cleared: false
:source_hash: dac8590

#import os  # library to get directory and file paths
#import tarfile # this module makes possible to read and write tar archives
#
#def extract_data(filename, where):
#    datadir = os.path.join(where) # directory where extract all datafile
#    if not os.path.exists(datadir): # check if this directory exists
#       print("Extracting data...")
#       tar_path = os.path.join(filename)  # path to the tgz file
#       with tarfile.open(tar_path, mode='r:gz') as data: # open the tgz file
#          data.extractall(datadir)  # extract all data file in datadir
#            
#extract_data('data/nycflights.tar.gz','data')
```

+++ {"cell_id": "00062-b0b0067f-5f65-42ab-9c3d-eec373377211", "deepnote_cell_type": "markdown", "output_cleared": false}

### Read one file with `pandas.read_csv` and compute mean departure delay

```{code-cell} ipython3
:cell_id: 00063-571dc5fc-adc1-4c78-8327-8789e66df803
:deepnote_cell_type: code
:execution_millis: 1287
:execution_start: 1604928326000
:output_cleared: false
:source_hash: 1e7ce2bd

import pandas as pd
df = pd.read_csv(os.path.join("data", "nycflights",'1990.csv'))
df.head()
```

```{code-cell} ipython3
:cell_id: 00064-89e8646e-fc15-431f-a829-4f996610ba3e
:deepnote_cell_type: code
:execution_millis: 11
:execution_start: 1604928328770
:output_cleared: false
:source_hash: 5c6e1e22

# What is the schema?
df.dtypes
```

```{code-cell} ipython3
:cell_id: 00065-e7b82501-4d4b-4897-8c54-60be7eb78e78
:deepnote_cell_type: code
:execution_millis: 19
:execution_start: 1604928330437
:output_cleared: false
:source_hash: 8b27f206

# What originating airports are in the data?
df.Origin.unique()
```

```{code-cell} ipython3
:cell_id: 00066-c5b73f9f-634e-4292-aa87-a037ad381e1b
:deepnote_cell_type: code
:execution_millis: 15
:execution_start: 1604928331514
:output_cleared: false
:source_hash: c59d6c33

# Mean departure delay per-airport for one year
df.groupby('Origin').DepDelay.mean()
```

+++ {"cell_id": "00067-6f8e0f0d-ad3d-40e2-890d-7adaf5d875e1", "deepnote_cell_type": "markdown", "output_cleared": false}

### Sequential code: Mean Departure Delay Per Airport

The above cell computes the mean departure delay per-airport for one year. Here we expand that to all years using a sequential for loop.

```{code-cell} ipython3
:cell_id: 00068-55d4865b-71d5-43e5-9d57-bd68e47f7bd6
:deepnote_cell_type: code
:execution_millis: 5
:execution_start: 1604928333654
:output_cleared: false
:source_hash: 3418c48a

from glob import glob
filenames = sorted(glob(os.path.join('data', "nycflights", '*.csv')))
filenames
```

```{code-cell} ipython3
:cell_id: 00069-57244563-895c-4b4e-babf-722e96a45882
:deepnote_cell_type: code
:execution_millis: 9916
:execution_start: 1604928335284
:output_cleared: false
:source_hash: d67fb36f

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
:cell_id: 00070-7408a639-3dfc-4e36-8f9d-1aad78f8be32
:deepnote_cell_type: code
:execution_millis: 1
:execution_start: 1604928345215
:output_cleared: false
:source_hash: 2f430a9c

mean
```

+++ {"cell_id": "00071-642685f8-bb57-4d34-8a9a-c1befd4aeadd", "deepnote_cell_type": "markdown", "output_cleared": false}

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

+++ {"cell_id": "00072-2512cd85-75f2-493d-84ec-811f227e9e0b", "deepnote_cell_type": "markdown", "output_cleared": false}

[Delayed best practices](https://docs.dask.org/en/latest/delayed-best-practices.html)
