odc.stats
=========

Statistical product generation framework.

Installation
------------

```
pip install --extra-index-url="https://packages.dea.ga.gov.au" odc-stats
```

Usage
-----
Stats offers a set of tools to generate clear pixel count and geometric median on Sentinel-2 and Landsat satellite images.
### Steps to run stats

### 1- Save tasks

From a machine that has access to your database, run:

```
odc-stats save-tasks --year YYYY --grid  grid-name  --temporal-range range product-name
```
This will generate a csv file containing the list of tasks to be processed, a cache file that will be used as input to the stats cli, and geojson files.  



### 2- Publish tasks 

```
odc-stats publish-tasks cache-file-path queue-name task-id/index 
```
This function populates  stats queue from a cache file. Each message contains a tile id. An example of a tile id is shown below:

```x+003/y+004/2019--P1Y```  

One can publish a set of tasks based on their indexes from the csv file.  For example, if your csv file contains tasks across multiple years, you can just publish tasks belonging to a particular year by providing a range, for example:

```
tasks=$(shuf -i  1000-2000)
```

where 1000 to 2000 covers the indexes for a single year.

odc-stats publish-tasks DB-file-path queue-name  tasks --dryrun
Alway run with the dryrun option and check tasks against the csv file to ensure you're publishing correct indexes.

### 3- Run stats
Sample command:
```
odc-stats run path_to_cache_file  2019--P1Y/31/24 --threads=96 --memory-limit=700Gi --max-procesing-time 3600 --config cfg.txt --location file://localpath/
```  

Where cfg.txt contains the following configurations:
```
plugin: gm-s2
max_processing_time: 3600
job_queue_max_lease: 300
renew_safety_margin: 60
future_poll_interval: 2
s3_acl: bucket-owner-full-control
# Generic product attributes
cog_opts:
    zlevel: 6
    overrides:
    rgba:
        compress: JPEG             
        jpeg_quality: 90                     
product:
    name: gm_s2_annual
    short_name: gm_s2_annual
    version: 1.0.0
    product_family: geomedian
    collections_site: explorer.digitalearth.africa
    producer: digitalearthafrica.org
    region_code_format: "x{x:03d}y{y:03d}"
```

Note that configurations are very different for different products.  Samples of plugins configurations can be found in the following repository. 
```
https://bitbucket.org/geoscienceaustralia/datakube-apps/src/master/workspaces/deafrica-prod-af/processing/
```

Notes:  Stats on large datasets are run using K8 jobs.  Examples for Sentinel-2 can be found in the following locations:

https://bitbucket.org/geoscienceaustralia/datakube-apps/src/master/workspaces/deafrica-prod-af/processing/06_stats_gm_16xl.yaml
https://bitbucket.org/geoscienceaustralia/datakube-apps/src/master/workspaces/deafrica-prod-af/processing/06_stats_pq.yaml

