# Python tools for Big Data

Notebooks for [Master of Data Science Rennes](https://www.sites.univ-rennes2.fr/master-mas/index.html)

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/pnavaro/big-data/master)
[![Book](https://github.com/pnavaro/big-data//workflows/book/badge.svg)](https://pnavaro.github.io/big-data)
[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-Ready--to--Code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/pnavaro/big-data)


A [website](https://pnavaro.github.io/big-data) generated with [jupyterbook](https://jupyterbook.org) is available.

## Run Jupyter notebooks with docker

### Get docker app
 - [Mac](https://www.docker.com/docker-mac)
 - [Windows](https://www.docker.com/docker-windows)
 - [Linux](https://runnable.com/docker/install-docker-on-linux)

You can run these notebooks with Docker. The following command starts a container with the Notebook 
server listening for HTTP connections on port 8888 and 4040 without authentication configured.

```
git clone https://github.com/pnavaro/big-data.git
docker run --rm -v $PWD/big-data:/home/jovyan/ -p 8888:8888 -p 4040:4040 pnavaro/big-data
```

## References

### Books
  * [Python for Data Analysis](https://github.com/wesm/pydata-book) by Wes McKinney.
  * [Python Data Science Handbook](https://github.com/jakevdp/PythonDataScienceHandbook) by Jake VanderPlas
  
### Software documentation
  * [Pandas](http://pandas.pydata.org).
  * [Dask](https://dask.pydata.org/)
  * [PySpark](http://spark.apache.org/docs/latest/api/python/index.html)
  * [Apache Arrow](https://arrow.apache.org/docs/python/index.html)
  * [Parquet](https://parquet.apache.org)
  * [GCSFS](http://gcsfs.readthedocs.io/en/latest/)
  * [Dask.distributed](https://distributed.readthedocs.io/en/latest/)
  * [fastparquet](https://fastparquet.readthedocs.io/en/latest/)

### Tutorials
  - Python
  	   * [Analyzing and Manipulating Data with Pandas Beginner](https://youtu.be/6ohWS7J1hVA): SciPy 2016 Tutorial by Jonathan Rocher.
  - Dask
      * [Dask Examples](https://examples.dask.org)
      * [Parallel Data Analysis with Dask](https://github.com/TomAugspurger/dask-tutorial-pycon-2018)  Dask tutorial at PyCon 2018 by Tom Augspurger.
  	   * [Parallelizing Scientific Python with Dask](https://www.youtube.com/watch?v=mqdglv9GnM8) SciPy 2018 Tutorial by James Crist and Martin Durant
  	   * [Parallelizing Scientific Python with Dask](https://youtu.be/mbfsog3e5DA), SciPy 2017 Tutorial by James Crist.
  	   * [Parallel Python: Analyzing Large Datasets Intermediate](https://www.youtube.com/watch?v=5Md_sSsN51k), SciPy 2016 Tutorial by Matthew Rocklin.
  	   * [Parallel Data Analysis in Python](https://www.youtube.com/watch?v=a8gpcnmggiU), SciPy 2017 Tutorial by Matthew Rocklin, Ben Zaitlen & Aron Ahmadia.
       * [Matthew Rocklin - Streaming Processing with Dask](https://www.youtube.com/watch?v=yI_yZoUaz60)
       * [Jacob Tomlinson - Dask Video Tutorial 2020](https://github.com/jacobtomlinson/dask-video-tutorial-2020)
  - Hadoop
  	   * [Writing an Hadoop MapReduce Program in Python](http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/) by Michael G. Noll.
  - Spark
  	   * [GetOting Started with Apache Spark Tutorial - Databricks](https://databricks.com/product/getting-started-guide)
  	   * [Hortonworks Data Tutorials](https://github.com/hortonworks/data-tutorials)

### Blog posts
  * [Don't use Hadoop - your data isn't that big](https://www.chrisstucchio.com/blog/2013/hadoop_hatred.html)
  * [Format Wars: From VHS and Beta to Avro and Parquet](http://www.svds.com/dataformats/)  overview of Hadoop File formats.
  * [Should you replace Hadoop with your laptop?](http://veekaybee.github.io/2017/03/20/hadoop-or-laptop/) by Vicki Boykis.
  * [Implementing MapReduce with multiprocessing](https://pymotw.com/2/multiprocessing/mapreduce.html) by Doug Hellmann.
  * [Deploying Dask on YARN](https://jcrist.github.io/dask-on-yarn) by Jim Crist.
  * [Native Hadoop file system (HDFS) connectivity in Python](http://wesmckinney.com/blog/python-hdfs-interfaces/) by Wes McKinney.
  * [Working Notes from Matthew Rocklin](http://matthewrocklin.com/blog) (must read)
  * [Large SVDs with Dask](https://t.co/6IBXNwyfbI)
  * [Machine Learning – 7 astuces pour scaler Python sur de grands datasets](https://blog.octo.com/machine-learning-7-astuces-pour-scaler-python-sur-de-grands-datasets/)


### Online courses
  * [DataCamp Cheat Sheets](https://www.datacamp.com/community/data-science-cheatsheets)
  * [Outils pour le Big Data](https://perso.univ-rennes1.fr/pierre.nerzic/Hadoop/) by Pierre Nerzic. 🇫🇷
  * [wikistat - Ateliers Big Data](https://github.com/wikistat/Ateliers-Big-Data) by Philippe Besse. 🇫🇷
  * [Data Science and Big Data with Python](https://github.com/phelps-sg/python-bigdata) by Steve Phelps.

Pierre

<a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/">Creative Commons Attribution-NonCommercial 4.0 International License</a>.
