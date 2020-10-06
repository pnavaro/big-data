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

# Dask bag

Dask proposes "big data" collections with a small set of high-level primitives like `map`, `filter`, `groupby`, and `join`.  With these common patterns we can often handle computations that are more complex than map, but are still structured.

- Dask-bag excels in processing data that can be represented as a sequence of arbitrary inputs ("messy" data)
- When you encounter a set of data with a format that does not enforce strict structure and datatypes.

**Related Documentation**

*  [Bag Documenation](http://dask.pydata.org/en/latest/bag.html)
*  [Bag API](http://dask.pydata.org/en/latest/bag-api.html)

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
data = list(range(1,9))
data
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import dask.bag as db

b = db.from_sequence(data)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
b.compute()  # Gather results back to local process
```

```{code-cell} ipython3
b.map(lambda x : x//2).compute() # compute length of each element and collect results
```

```{code-cell} ipython3
from time import sleep

def slow_half( x):
    sleep(1)
    return x // 2

res = b.map(slow_half)
res
```

```{code-cell} ipython3
%%time
res.compute()
```

```{code-cell} ipython3
res.visualize()
```

```{code-cell} ipython3
b.topk
```

```{code-cell} ipython3
b.product(b).compute() # Cartesian product of each pair 
# of elements in two sequences (or the same sequence in this case)
```

+++ {"slideshow": {"slide_type": "slide"}}

Chain operations to construct more complex computations

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
(b.filter(lambda x: x % 2 > 0)
  .product(b)
  .filter( lambda v : v[0] % v[1] == 0 and v[0] != v[1])
  .compute())
```

+++ {"slideshow": {"slide_type": "slide"}}

## Daily stock example

Let's use the bag interface to read the json files containing time series.

Each line is a JSON encoded dictionary with the following keys
- timestamp: Day.
- close: Stock value at the end of the day.
- high: Highest value.
- low: Lowest value.
- open: Opening price.

```{code-cell} ipython3
# preparing data
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
%ls data/daily-stock/*.json
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import dask.bag as db
import json
stocks = db.read_text('data/daily-stock/*.json')
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
stocks.npartitions
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
stocks.visualize()
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
import json
js = stocks.map(json.loads)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
import os, sys
from glob import glob
import pandas as pd
import json

here = os.getcwd() # get the current directory
filenames = sorted(glob(os.path.join(here,'data', 'daily-stock', '*.json')))
filenames
```

```{code-cell} ipython3
%rm data/daily-stock/*.h5
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
from tqdm.notebook import tqdm
for fn in tqdm(filenames):
    with open(fn) as f:
        data = [json.loads(line) for line in f]
        
    df = pd.DataFrame(data)
    
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
filenames = sorted(glob(os.path.join(here,'data', 'daily-stock', '*.h5')))
filenames
```

+++ {"slideshow": {"slide_type": "slide"}}

### Serial version

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
series = {}
for fn in filenames:   # Simple map over filenames
    series[fn] = pd.read_hdf(fn)['close']

results = {}

for a in filenames:    # Doubly nested loop over the same collection
    for b in filenames:  
        if a != b:     # Filter out bad elements
            results[a, b] = series[a].corr(series[b])  # Apply function

((a, b), corr) = max(results.items(), key=lambda kv: kv[1])  # Reduction
```

```{code-cell} ipython3
a, b, corr
```

+++ {"slideshow": {"slide_type": "slide"}}

## Dask.bag methods

We can construct most of the above computation with the following dask.bag methods:

*  `collection.map(function)`: apply function to each element in collection
*  `collection.product(collection)`: Create new collection with every pair of inputs
*  `collection.filter(predicate)`: Keep only elements of colleciton that match the predicate function
*  `collection.max()`: Compute maximum element

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
%%time

import dask.bag as db

b = db.from_sequence(filenames)
series = b.map(lambda fn: pd.read_hdf(fn)['close'])

corr = (series.product(series)
              .filter(lambda ab: not (ab[0] == ab[1]).all())
              .map(lambda ab: ab[0].corr(ab[1])).max())
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
%%time

result = corr.compute()
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
result
```

+++ {"slideshow": {"slide_type": "slide"}}

### Wordcount with Dask bag

```{code-cell} ipython3
import lorem

lorem.text()
```

```{code-cell} ipython3
import lorem

for i in range(20):
    with open(f"sample{i:02d}.txt","w") as f:
        f.write(lorem.text())
```

```{code-cell} ipython3
%ls *.txt
```

```{code-cell} ipython3
import glob
glob.glob('sample*.txt')
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import dask.bag as db
import glob
b = db.read_text(glob.glob('sample*.txt'))

wordcount = (b.str.replace("."," ")  # remove dots
             .str.lower()           # lower text
             .str.strip()           # remove \n and trailing spaces
             .str.split()           # split into words
             .flatten()             # chain all words lists
             .frequencies()         # compute occurences
             .topk(10, lambda x: x[1])) # sort and return top 10 words


wordcount.compute() # Run all tasks and return result
```

+++ {"slideshow": {"slide_type": "slide"}}

## Genome example
We will use a Dask bag to calculate the frequencies of sequences of five bases, and then sort the sequences into descending order ranked by their frequency.

- First we will define some functions to split the bases into sequences of a certain size

+++ {"slideshow": {"slide_type": "fragment"}}

### Exercise 9.1

- Implement a function `group_characters(line, n=5)` to group `n` characters together and return a iterator. `line` is a text line in genome.txt file.

```py
>>> line = "abcdefghijklmno"
>>> for seq in group_character(line, 5):
        print(seq)
        
"abcde"
"efghi"
"klmno"
```

    
- Implement `group_and_split(line)`
```py
>>> group_and_split('abcdefghijklmno')
['abcde', 'fghij', 'klmno']
```

- Use the dask bag to compute  the frequencies of sequences of five bases.

```{code-cell} ipython3
import os
from glob import glob

data_path = os.path.join("data")
with open(os.path.join(data_path,"genome.txt")) as g:
    data = g.read()
    for i in range(8):
        file = os.path.join(data_path,f"genome{i:02d}.txt")
        with open(file,"w") as f:
            f.write(data)

glob("data/genome0*.txt")
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 9.2

The [FASTA](http://www.cbs.dtu.dk/services/NetGene2/fasta.php) file format is used to write several genome sequences.

- Create a function that can read a [FASTA file](../data/nucleotide-sample.txt) and compute the frequencies for n = 5 of a given sequence.

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 9.3

Write a program that uses the function implemented above to read several FASTA files stored in a Dask bag.

+++ {"slideshow": {"slide_type": "slide"}}

## Some remarks about bag

*  Higher level dask collections include functions for common patterns
*  Move data to collection, construct lazy computation, trigger at the end
*  Use Dask.bag (`product + map`) to handle nested for loop

Bags have the following known limitations

1.  Bag operations tend to be slower than array/dataframe computations in the
    same way that Python tends to be slower than NumPy/Pandas
2.  ``Bag.groupby`` is slow.  You should try to use ``Bag.foldby`` if possible.
    
3. Check the [API](http://dask.pydata.org/en/latest/bag-api.html) 

4. `dask.dataframe` can be faster than `dask.bag`.  But sometimes it is easier to load and clean messy data with a bag. We will see later how to transform a bag into a `dask.dataframe` with the [to_dataframe](http://dask.pydata.org/en/latest/bag-api.html#dask.bag.Bag.to_dataframe) method.
