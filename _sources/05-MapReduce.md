---
jupytext:
  cell_metadata_json: true
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

# Map Reduce

This notebook objective is to code in Python language a wordcount application using map-reduce process. A java version is well explained on [this page](https://www.dezyre.com/hadoop-tutorial/hadoop-mapreduce-wordcount-tutorial)

![domain decomposition](images/domain_decomp.png)

credits: https://computing.llnl.gov/tutorials/parallel_comp

+++ {"slideshow": {"slide_type": "slide"}}


## `map` function example

The `map(func, seq)` Python function applies the function func to all the elements of the sequence seq. It returns a new list with the elements changed by func

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
def f(x):
    return x * x

rdd = [2, 6, -3, 7]
res = map(f, rdd )
res  # Res is an iterator
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
print(*res)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
from operator import mul
rdd1, rdd2 = [2, 6, -3, 7], [1, -4, 5, 3]
res = map(mul, rdd1, rdd2 ) # element wise sum of rdd1 and rdd2 
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
print(*res)
```

+++ {"slideshow": {"slide_type": "slide"}}

![MapReduce](images/mapreduce.jpg)

+++ {"slideshow": {"slide_type": "slide"}}

## `functools.reduce` example

The function `reduce(func, seq)` continually applies the function func() to the sequence seq and return a single value. For example, reduce(f, [1, 2, 3, 4, 5]) calculates f(f(f(f(1,2),3),4),5).

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from functools import reduce
from operator import add

reduce(mul, rdd) # computes ((((1+2)+3)+4)+5)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
p = 1
for x in rdd:
    p *= x
p
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
def g(x,y):
    return x * y

reduce(g, rdd) 
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
p = 1
for x in rdd:
    p *= x
p
```

+++ {"slideshow": {"slide_type": "slide"}}

## Weighted mean and Variance

If the generator of random variable $X$ is discrete with probability mass function $x_1 \mapsto p_1, x_2 \mapsto p_2, \ldots, x_n \mapsto p_n$ then

$$\operatorname{Var}(X) = \left(\sum_{i=1}^n p_i x_i ^2\right) - \mu^2,$$

where $\mu$ is the average value, i.e.

$$\mu = \sum_{i=1}^n p_i x_i. $$

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
X = [5, 1, 2, 3, 1, 2, 5, 4]
P = [0.05, 0.05, 0.15, 0.05, 0.15, 0.2, 0.1, 0.25]
```

Example of `zip`

```{code-cell} ipython3
for x, p in zip(X, P):
    print(f" x = {x} ..... p = {p}")
```

```{code-cell} ipython3
from itertools import zip_longest

for x, p in zip_longest(X, [0.1], fillvalue=0.0):
    print(f" x = {x} ..... p = {p}") 
```

```{code-cell} ipython3
sum(P)
```

+++ {"slideshow": {"slide_type": "fragment"}}

### Exercise 5.1

- Write functions to compute the average value and variance using for loops

+++ {"slideshow": {"slide_type": "fragment"}}

### Exercise 5.2

- Write functions to compute the average value and variance using `map` and `reduce`

+++

### Examples with filter

```{code-cell} ipython3
res = filter( lambda p: p > 0.1, P)  # select p > 0.1
print(*res)
```

```{code-cell} ipython3
res = filter( lambda x: x % 3 == 0, range(15)) # select integer that can be divided by 3
print(*res)
```

+++ {"slideshow": {"slide_type": "fragment"}}

*NB: Exercises above are just made to help to understand map-reduce process.
This is a bad way to code a variance in Python. You should use [Numpy](http://www.numpy.org) instead.*

+++ {"slideshow": {"slide_type": "slide"}}

## Wordcount 

We will modify the `wordcount` application into a map-reduce process.

The `map` process takes text files as input and breaks it into words. The `reduce`  process sums the counts for each word and emits a single key/value with the word and sum.

We need to split the wordcount function we wrote in notebook 04 in order to use map and reduce. 

In the following exercices we will implement in Python the Java example described in [Hadoop documentation](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0).

+++ {"slideshow": {"slide_type": "slide"}}

## Map - Read file and return a key/value pairs

### Exercise 5.3

Write a function `mapper` with a single file name as input that returns a sorted sequence of tuples (word, 1) values.

```pybt
mapper('sample.txt')
[('adipisci', 1), ('adipisci', 1), ('adipisci', 1), ('adipisci', 1), ('adipisci', 1), ('adipisci', 1), ('adipisci', 1), ('aliquam', 1), ('aliquam', 1), ('aliquam', 1), ('aliquam', 1), ('aliquam', 1), ('aliquam', 1), ('aliquam', 1), ('amet', 1), ('amet', 1), ('amet', 1)...
```

+++

## Partition

### Exercise 5.4

Create a function named `partitioner` that stores the key/value pairs from `mapper`  that group (word, 1) pairs into a list as:
```python
partitioner(mapper('sample.txt'))
[('adipisci', [1, 1, 1, 1, 1, 1, 1]), ('aliquam', [1, 1, 1, 1, 1, 1, 1]), ('amet', [1, 1, 1, 1],...]
```

+++ {"slideshow": {"slide_type": "slide"}}

## Reduce - Sums the counts and returns a single key/value (word, sum).

### Exercice 5.5

Write the function `reducer` that read a tuple (word,[1,1,1,..,1]) and sum the occurrences of word to a final count, and then output the tuple (word,occurences).

```python
reducer(('hello',[1,1,1,1,1])
('hello',5)
```

+++ {"slideshow": {"slide_type": "slide"}}

## Process several files

Let's create 8 files `sample[0-7].txt`. Set most common words at the top of the output list.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from lorem import text
for i in range(8):
    with open("sample{0:02d}.txt".format(i), "w") as f:
        f.write(text())
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
import glob
files = sorted(glob.glob('sample0*.txt'))
files
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 5.6
- Use functions implemented above to count (word, occurences) by using a for loops over files and partitioned data.

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 5.7
- This time use `map` function to apply mapper and reducer.
