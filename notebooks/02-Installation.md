---
jupytext:
  cell_metadata_json: true
  encoding: '# -*- coding: utf-8 -*-'
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

# Installation

Conda is a powerful package manager and environment manager.

[Ref: Getting started with conda](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html)

<!-- #region {"slideshow": {"slide_type": "slide"}} -->
##  Install [Anaconda](https://www.anaconda.com/downloads) (large) or [Miniconda](https://conda.io/miniconda.html) (small) or [Miniforge](https://github.com/conda-forge/miniforge/releases) (best)

+++ {"slideshow": {"slide_type": "slide"}}

##  Open a terminal (Linux/MacOSX) or a Anaconda prompt (Windows)

+++ {"slideshow": {"slide_type": "fragment"}}

Verify that conda is installed and running on your system by typing:

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%bash
conda --version
```

+++ {"slideshow": {"slide_type": "slide"}}

Conda displays the number of the version that you have installed.

If you get an error message, make sure you closed and re-opened the
terminal window after installing, or do it now. 

To update conda to the current version. Type the following:

```bash
conda update -y conda -n base
```

+++ {"slideshow": {"slide_type": "slide"}}

## Managing channels

Conda channels are the locations where packages are stored. We use the [conda-forge](https://conda-forge.org),
a good community-led collection of recipes for conda. If you installed [Miniforge](https://github.com/conda-forge/miniforge) you already have a conda specific to conda-forge.

```bash
conda config --add channels conda-forge 
conda config --set channel_priority strict
```

Strict channel priority speed up conda operations and also reduce package incompatibility problems.

+++ {"slideshow": {"slide_type": "slide"}}

## Managing environments

Conda allows you to create separate environments containing files, packages,
and their dependencies that will not interact with other environments.

When you begin using conda, you already have a default environment named
``base``. You don't want to put programs into your base environment, though.
Create separate environments to keep your programs isolated from each other.

### Create a new environment and install a package in it.

We will name the environment `big-data` and install the version 3.8 of `python`. At the Anaconda Prompt or in your terminal window, type the following:
```bash
conda create -y -n big-data python=3.8
```

+++ {"slideshow": {"slide_type": "slide"}}

### To use, or "activate" the new environment, type the following:

```bash
conda activate big-data
```

+++ {"slideshow": {"slide_type": "fragment"}}

Now that you are in your ``big-data`` environment, any conda commands you type will go to that environment until you deactivate it.

Verify which version of Python is in your current environment:

```bash
python --version
```

+++ {"slideshow": {"slide_type": "slide"}}

### To see a list of all your environments, type:

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%bash
conda info --envs
```

+++ {"slideshow": {"slide_type": "slide"}}

The active environment is the one with an asterisk (*).

### Change your current environment back to the default (base):

```bash
conda activate
```

+++ {"slideshow": {"slide_type": "slide"}}

## Managing packages

- Check to see if a package you have not installed named "jupyter" is available from the Anaconda repository (must be connected to the Internet):

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%bash
conda search jupyter | grep conda-forge
```

+++ {"slideshow": {"slide_type": "slide"}}

Conda displays a list of all packages with that name on conda-forge repository, so we know it is available.

Install this package into the base environment:

```bash
conda activate
conda install -y jupyter -c conda-forge -n base
```

+++ {"slideshow": {"slide_type": "slide"}}

Check to see if the newly installed program is in this environment:

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%bash
conda list jupyter
```

+++ {"slideshow": {"slide_type": "slide"}}

### Update a new conda environment from file

Download the file [environment.yml](https://raw.githubusercontent.com/pnavaro/big-data/master/environment.yml).
This file contains the packages list for this course. Be aware that it takes time to download and install all packages.

```bash
conda env update -f environment.yml -n big-data
```

[Conda envs documentation](https://conda.io/docs/using/envs.html).

+++ {"slideshow": {"slide_type": "slide"}}

Activating the conda environment will change your shell’s prompt to show what virtual environment you’re using, and modify the environment so that running python will get you that particular version and installation of Python. 
<pre>
$ conda activate big-data
(big-data) $ python
Python 3.6.2 (default, Jul 17 2017, 16:44:45) 
[GCC 4.2.1 Compatible Apple LLVM 8.1.0 (clang-802.0.42)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> quit()
</pre>

**You must do this everytime you open a new terminal**

+++ {"slideshow": {"slide_type": "slide"}}

## Install the kernel for jupyter

```bash
conda run -n big-data python -m ipykernel install --user --name big-data
```

With this command you create the `big-data` kernel with python and all course dependencies.
The cell above will give you the path to the python that runs in this notebook.

```{code-cell} ipython3
---
slideshow:
  slide_type: slide
---
import sys
print(f"{sys.executable}")
```

```{code-cell} ipython3
---
slideshow:
  slide_type: fragment
---
%%bash
jupyter-kernelspec list
```

## Mamba

Mamba is a parallel reimplementation of the conda package manager in C++. It stays compatible as possible with conda interface. Install mamba from conda-forge:
```bash
conda install mamba -c conda-forge
```

To test it you can try to install the metapackage `r-tidyverse` which contains 144 packages.

```bash
$ time conda create -y r-tidyverse -n condatest
real	1m9.057s
$ time mamba create -y r-tidyverse -n mambatest
real	0m32.365s
```
In this comparison packages are already downloaded, mamba is even better with downloads.
