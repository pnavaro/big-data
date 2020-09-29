---
jupytext:
  cell_metadata_json: true
  encoding: '# -*- coding: utf-8 -*-'
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

# Wordcount

- [Wikipedia](https://en.wikipedia.org/wiki/Word_count)

- Word count example reads text files and counts how often words occur. 
- Word count is commonly used by translators to determine the price for the translation job.
- This is the "Hello World" program of Big Data.

+++ {"slideshow": {"slide_type": "slide"}}

Some recommendations:
- *Don't google too much, ask me or use the python documentation through `help` function.*
- *Do not try to find a clever or optimized solution, do something that works before.*
- *Please don't get the solution from your colleagues*
- *Notebooks will be updated every week with solutions*

+++ {"slideshow": {"slide_type": "slide"}}

## Create sample text file

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from lorem import text

with open("sample.txt", "w") as f:
    for i in range(10000):
        f.write(text())
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 4.1

Write a python program that counts the number of lines, different words and characters in that file.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%bash
wc sample.txt
du -h sample.txt
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 4.2

Create a function called `map_words` that take a file name as argument and return a lists containing all words as items.

```pytb
map_words("sample.txt")[:5] # first five words
['adipisci', 'adipisci', 'adipisci', 'adipisci', 'adipisci']
```

+++ {"slideshow": {"slide_type": "slide"}}

## Sorting a dictionary by value

By default, if you use `sorted` function on a `dict`, it will use keys to sort it.
To sort by values, you can use [operator](https://docs.python.org/3.6/library/operator.html).itemgetter(1)
Return a callable object that fetches item from its operand using the operand’s `__getitem__(` method. It could be used to sort results.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
import operator
fruits = [('apple', 3), ('banana', 2), ('pear', 5), ('orange', 1)]
getcount = operator.itemgetter(1)
dict(sorted(fruits, key=getcount))
```

+++ {"slideshow": {"slide_type": "fragment"}}

`sorted` function has also a `reverse` optional argument.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
dict(sorted(fruits, key=getcount, reverse=True))
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 4.3

Create a function `reduce` to reduce the list of words returned by `map_words` and return a dictionary containing all words as keys and number of occurrences as values.

```pybt
wordcount('sample.txt')
{'tempora': 2, 'non': 1, 'quisquam': 1, 'amet': 1, 'sit': 1}
```

+++ {"slideshow": {"slide_type": "slide"}}

You probably notice that this simple function is not easy to implement. Python standard library provides some features that can help.

+++ {"slideshow": {"slide_type": "slide"}}

## Container datatypes

`collection` module implements specialized container datatypes providing alternatives to Python’s general purpose built-in containers, `dict`, `list`, `set`, and `tuple`.

- `defaultdict` :	dict subclass that calls a factory function to supply missing values
- `Counter`	: dict subclass for counting hashable objects

+++ {"slideshow": {"slide_type": "slide"}}

### defaultdict

When you implement the `wordcount` function you probably had some problem to append key-value pair to your `dict`. If you try to change the value of a key that is not present 
in the dict, the key is not automatically created.

You can use a `try-except` flow but the `defaultdict` could be a solution. This container is a `dict` subclass that calls a factory function to supply missing values.
For example, using list as the default_factory, it is easy to group a sequence of key-value pairs into a dictionary of lists:

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from collections import defaultdict
s = [('yellow', 1), ('blue', 2), ('yellow', 3), ('blue', 4), ('red', 1)]
d = defaultdict(list)
for k, v in s:
    d[k].append(v)

dict(d)
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 4.4

- Modify the `reduce` function you wrote above by using a defaultdict with the most suitable factory.

+++ {"slideshow": {"slide_type": "slide"}}

### Counter

A Counter is a dict subclass for counting hashable objects. It is an unordered collection where elements are stored as dictionary keys and their counts are stored as dictionary values. Counts are allowed to be any integer value including zero or negative counts.

Elements are counted from an iterable or initialized from another mapping (or counter):

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
from collections import Counter

violet = dict(r=23,g=13,b=23)
print(violet)
cnt = Counter(violet)  # or Counter(r=238, g=130, b=238)
print(cnt['c'])
print(cnt['r'])
```

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
print(*cnt.elements())
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
cnt.most_common(2)
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
cnt.values()
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 4.5

Use a `Counter` object to count words occurences in the sample text file. 

+++ {"slideshow": {"slide_type": "fragment"}}

The Counter class is similar to bags or multisets in some Python libraries or other languages. We will see later how to use Counter-like objects in a parallel context. 

+++ {"slideshow": {"slide_type": "slide"}}

## Process multiple files

- Create several files containing `lorem` text named 'sample01.txt', 'sample02.txt'...
- If you process these files you return multiple dictionaries.
- You have to loop over them to sum occurences and return the resulted dict. To iterate on specific mappings, Python standard library provides some useful features in `itertools` module.
- [itertools.chain(*mapped_values)](https://docs.python.org/3.6/library/itertools.html#itertools.chain) could be used for treating consecutive sequences as a single sequence. 

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
import itertools, operator
fruits = [('apple', 3), ('banana', 2), ('pear', 5), ('orange', 1)]
vegetables = [('endive', 2), ('spinach', 1), ('celery', 5), ('carrot', 4)]
getcount = operator.itemgetter(1)
dict(sorted(itertools.chain(fruits,vegetables), key=getcount))
```

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 4.6

- Write the program that creates files, processes and use `itertools.chain` to get the merged word count dictionary.

+++ {"slideshow": {"slide_type": "slide"}}

### Exercise 4.7

- Create the `wordcount` function in order to accept several files as arguments and
return the result dict.

```
wordcount(file1, file2, file3, ...)
```

[Hint: arbitrary argument lists](https://docs.python.org/3/tutorial/controlflow.html#arbitrary-argument-lists)

+++ {"slideshow": {"slide_type": "slide"}}

- Example of use of arbitrary argument list and arbitrary named arguments.

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
def func( *args, **kwargs):
    for arg in args:
        print(arg)
        
    print(kwargs)
        
func( "3", [1,2], "bonjour", x = 4, y = "y")
```
