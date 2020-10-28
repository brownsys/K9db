# Reddit data
This directory contains scripts to set up per-user shards for reddit data

To work with the Reddit data, run the scripts in order, i.e.
Make sure to have at least 1TB of disk space available!

Adjust the limit setting in `03_shard.py`.

```
# download data
python3 01_download.py
python3 02_extract.py

export PYSPARK_PYTHON=python3.7
export PYSPARK_DRIVER_PYTHON=python3.7
mkdir -p ./sharded/db
spark-submit --driver-memory 380G 03_shard.py
```

##### Software required:
- python >= 3.7
- whichever python packages are necessary
- Spark 3.0.0

Questions @ `leonhard_spiegelberg@brown.edu`

(c) 2020