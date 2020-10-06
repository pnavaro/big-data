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

# Parallel Computation

This notebook objective is to learn how to parallelize application with Python

## Parallel computers
- Multiprocessor/multicore: several processors work on data stored in shared memory
- Cluster: several processor/memory units work together by exchanging data over a network
- Co-processor: a general-purpose processor delegates specific tasks to a special-purpose processor (GPU)

+++ {"slideshow": {"slide_type": "slide"}}

## Parallel Programming
- Decomposition of the complete task into independent subtasks and the data flow between them.
- Distribution of the subtasks over the processors minimizing the total execution time.
- For clusters: distribution of the data over the nodes minimizing the communication time.
- For multiprocessors: optimization of the memory access patterns minimizing waiting times.
- Synchronization of the individual processes.

+++ {"slideshow": {"slide_type": "slide"}}

## MapReduce

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from time import sleep
def process(x):
    sleep(1)
    return x*x
data = list(range(8))
data
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%time sum(process(x) for x in data)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%time sum(map(process,data))
```

+++ {"slideshow": {"slide_type": "slide"}}

## Multiprocessing 

`multiprocessing` is a package that supports spawning processes.

We can use it to display how many concurrent processes you can launch on your computer.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from multiprocessing import cpu_count

cpu_count()
```

+++ {"slideshow": {"slide_type": "slide"}}

## Futures

The `concurrent.futures` module provides a high-level interface for asynchronously executing callables.

The asynchronous execution can be performed with:
- **threads**, using ThreadPoolExecutor, 
- separate **processes**, using ProcessPoolExecutor. 
Both implement the same interface, which is defined by the abstract Executor class.

`concurrent.futures` can't launch **processes** on windows. Windows users must install 
[loky](https://github.com/tomMoral/loky).

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
%%file pmap.py
from concurrent.futures import ProcessPoolExecutor
from time import sleep, time

def process(x):
    sleep(1)
    return x*x

data = list(range(8))

if __name__ == '__main__':
    
    begin = time()
    with ProcessPoolExecutor() as pool:

        result = sum(pool.map(process, data))
    end = time()
    
    print(f"result = {result} and time = {end-begin}")
```

```{code-cell} ipython3
import sys
!{sys.executable} pmap.py
```

+++ {"slideshow": {"slide_type": "slide"}}

- `ProcessPoolExecutor` launches one slave process per physical core on the computer. 
- `pool.map` divides the input list into chunks and puts the tasks (function + chunk) on a queue.
- Each slave process takes a task (function + a chunk of data), runs map(function, chunk), and puts the result on a result list.
- `pool.map` on the master process waits until all tasks are handled and returns the concatenation of the result lists.

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
%%time
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as pool:

    results = sum(pool.map(process, data))
    
print(results)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Thread and Process: Differences

- A **process** is an instance of a running program. 
- **Process** may contain one or more **threads**, but a **thread** cannot contain a **process**.
- **Process** has a self-contained execution environment. It has its own memory space. 
- Application running on your computer may be a set of cooperating **processes**.
- **Process** don't share its memory, communication between **processes** implies data serialization.

- A **thread** is made of and exist within a **process**; every **process** has at least one **thread**. 
- Multiple **threads** in a **process** share resources, which helps in efficient communication between **threads**.
- **Threads** can be concurrent on a multi-core system, with every core executing the separate **threads** simultaneously.

+++ {"slideshow": {"slide_type": "slide"}}

## The Global Interpreter Lock (GIL)

- The Python interpreter is not thread safe.
- A few critical internal data structures may only be accessed by one thread at a time. Access to them is protected by the GIL.
- Attempts at removing the GIL from Python have failed until now. The main difficulty is maintaining the C API for extension modules.
- Multiprocessing avoids the GIL by having separate processes which each have an independent copy of the interpreter data structures.
- The price to pay: serialization of tasks, arguments, and results.

+++ {"slideshow": {"slide_type": "slide"}}

## Parallelize text files downloads

- Victor Hugo http://www.gutenberg.org/files/135/135-0.txt
- Marcel Proust http://www.gutenberg.org/files/7178/7178-8.txt
- Emile Zola http://www.gutenberg.org/files/1069/1069-0.txt
- Stendhal http://www.gutenberg.org/files/44747/44747-0.txt

### Exercise 6.1

Use `ThreadPoolExecutor` to parallelize the code above.

```{code-cell} ipython3
%mkdir -p books
```

```{code-cell} ipython3
%%time
import urllib.request as url
source = "https://mmassd.github.io/"  # "http://svmass2.mass.uhb.fr/hub/static/datasets/"
url.urlretrieve(source+"books/hugo.txt",     filename="books/hugo.txt")
url.urlretrieve(source+"books/proust.txt",   filename="books/proust.txt")
url.urlretrieve(source+"books/zola.txt",     filename="books/zola.txt")
url.urlretrieve(source+"books/stendhal.txt", filename="books/stendhal.txt")
```

+++ {"slideshow": {"slide_type": "slide"}}

## Wordcount

```{code-cell} ipython3
from glob import glob
from collections import defaultdict
from operator import itemgetter
from itertools import chain
from concurrent.futures import ThreadPoolExecutor

def mapper(filename):
    " split text to list of key/value pairs (word,1)"
    with open(filename) as f:
        data = f.read()
        
    data = data.strip().replace(".","").lower().split()
        
    return sorted([(w,1) for w in data])

def partitioner(mapped_values):
    """ get lists from mapper and create a dict with
    (word,[1,1,1])"""
    
    res = defaultdict(list)
    for w, c in mapped_values:
        res[w].append(c)
        
    return res.items()

def reducer( item ):
    """ Compute words occurences from dict computed
    by partioner
    """
    w, v = item
    return (w,len(v))
```

+++ {"slideshow": {"slide_type": "slide"}}

## Parallel reduce

- For parallel reduce operation, data must be aligned in a container. We already created a `partitioner` function that returns this container.

### Exercise 6.2

Write a parallel program that uses the three functions above using `ThreadPoolExecutor`. It reads all the "sample\*.txt" files. Map and reduce steps are parallel.

+++ {"slideshow": {"slide_type": "slide"}}

## Increase volume of data

*Due to the proxy, code above is not runnable on workstations*

### Getting the data

- [The Latin Library](http://www.thelatinlibrary.com/) contains a huge collection of freely accessible Latin texts. We get links on the Latin Library's homepage ignoring some links that are not associated with a particular author.

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
from bs4 import BeautifulSoup  # web scraping library
from urllib.request import *

base_url = "http://www.thelatinlibrary.com/"
home_content = urlopen(base_url)

soup = BeautifulSoup(home_content, "lxml")
author_page_links = soup.find_all("a")
author_pages = [ap["href"] for i, ap in enumerate(author_page_links) if i < 49]
```

+++ {"slideshow": {"slide_type": "slide"}}

### Generate html links

- Create a list of all links pointing to Latin texts. The Latin Library uses a special format which makes it easy to find the corresponding links: All of these links contain the name of the text author.

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
ap_content = list()
for ap in author_pages:
    ap_content.append(urlopen(base_url + ap))

book_links = list()
for path, content in zip(author_pages, ap_content):
    author_name = path.split(".")[0]
    ap_soup = BeautifulSoup(content, "lxml")
    book_links += ([link for link in ap_soup.find_all("a", {"href": True}) if author_name in link["href"]])
```

+++ {"slideshow": {"slide_type": "slide"}}

### Download webpages content

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
from urllib.error import HTTPError

num_pages = 100

for i, bl in enumerate(book_links[:num_pages]):
    print("Getting content " + str(i + 1) + " of " + str(num_pages), end="\r", flush=True)
    try:
        content = urlopen(base_url + bl["href"]).read()
        with open(f"book-{i:03d}.dat","wb") as f:
            f.write(content)
    except HTTPError as err:
        print("Unable to retrieve " + bl["href"] + ".")
        continue
```

+++ {"slideshow": {"slide_type": "slide"}}

### Extract data files

- I already put the content of pages in files named book-*.txt
- You can extract data from the archive by running the cell below

+++ {"slideshow": {"slide_type": "fragment"}}

```py
import os  # library to get directory and file paths
import tarfile # this module makes possible to read and write tar archives

def extract_data():
    datadir = os.path.join('data','latinbooks')
    if not os.path.exists(datadir):
       print("Extracting data...")
       tar_path = os.path.join('data', 'latinbooks.tgz')
       with tarfile.open(tar_path, mode='r:gz') as books:
          books.extractall('data')
            
extract_data() # this function call will extract text files in data/latinbooks
```

+++ {"slideshow": {"slide_type": "slide"}}

### Read data files

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from glob import glob
files = glob('book*.dat')
texts = list()
for file in files:
    with open(file,'rb') as f:
        text = f.read()
    texts.append(text)
```

+++ {"slideshow": {"slide_type": "slide"}}

### Extract the text from html and split the text at periods to convert it into sentences.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%time
from bs4 import BeautifulSoup

sentences = list()

for i, text in enumerate(texts):
    print("Document " + str(i + 1) + " of " + str(len(texts)), end="\r", flush=True)
    textSoup = BeautifulSoup(text, "lxml")
    paragraphs = textSoup.find_all("p", attrs={"class":None})
    prepared = ("".join([p.text.strip().lower() for p in paragraphs[1:-1]]))
    for t in prepared.split("."):
        part = "".join([c for c in t if c.isalpha() or c.isspace()])
        sentences.append(part.strip())

# print first and last sentence to check the results
print(sentences[0])
print(sentences[-1])
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 6.3

Parallelize this last process using `concurrent.futures`.

+++ {"slideshow": {"slide_type": "slide"}}

## References

- [Using Conditional Random Fields and Python for Latin word segmentation](https://medium.com/@felixmohr/using-python-and-conditional-random-fields-for-latin-word-segmentation-416ca7a9e513)
