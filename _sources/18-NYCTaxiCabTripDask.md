---
jupytext:
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

# Dask dataframes on HDFS

To use Dask dataframes in parallel across an HDFS cluster to read CSV data. We can coordinate these computations with [distributed](http://distributed.dask.org/en/latest/) and dask.dataframe.

As Spark, Dask can work in cluster mode. You can use the dask module [dask_jobqueue](https://jobqueue.dask.org/en/latest/) to launch a Dask cluster with the job manager SLURM.

+++

```py
from dask_jobqueue import SLURMCluster

cluster = SLURMCluster(cores=16,
                       queue='test',
                       project='myproject',
                       memory="16GB",
                       walltime="01:00:00")
```

+++

The cluster generates a traditional job script and submits that an appropriate number of times to the job queue. You can see the job script that it will generate as follows:

```{code-cell} ipython3
print(cluster.job_script())
```

Access to the cluster using following lines:

```
import dask.dataframe as dd
from dask.distributed import Client
client = Client(cluster)
```

`nyc2014` is a dask.dataframe objects which present a subset of the Pandas API to the user, but farm out all of the work to the many Pandas dataframes they control across the network.

```python
nyc2014 = dd.read_csv('/opt/datasets/nyc-data/2014/yellow*.csv',
parse_dates=['pickup_datetime', 'dropoff_datetime'],
skipinitialspace=True)
nyc2014 = c.persist(nyc2014)
progress(nyc2014)
```

+++

## Exercises 

- Display head of the dataframe
- Display number of rows of this dataframe.
- Compute the total number of passengers.
- Count occurrences in the payment_type column both for the full dataset, and filtered by zero tip (tip_amount == 0).
- Create a new column, tip_fraction
- Plot the average of the new column tip_fraction grouped by day of week.
- Plot the average of the new column tip_fraction grouped by hour of day.

[Dask dataframe documentation](http://docs.dask.org/en/latest/dataframe.html)
