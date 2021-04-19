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
Stats offers a set of tools to generate clear pixel count and geometric median on Sentinel-2 and Landsat satellite images and it's currently being extended to support sibling products.

### Steps to run stats

### 1- Save tasks

From your sandbox (or a machine that has access to your database), run:

```
odc-stats save-tasks --frequency annual --temporal-range 2015--P6Y --grid au-10  ga_ls8c_ard_3
```

The above command will generate a csv file containing a list of tasks for years between 2015 to 2020, a cache file that will be used as an input to the stats cli, and geojson files.  

The above command will generate the following files:

```
ga_ls8c_ard_3_2015--P6Y-2015--P1Y.geojson  
ga_ls8c_ard_3_2015--P6Y-2016--P1Y.geojson  
ga_ls8c_ard_3_2015--P6Y-2017--P1Y.geojson  
ga_ls8c_ard_3_2015--P6Y-2018--P1Y.geojson
ga_ls8c_ard_3_2015--P6Y-2019--P1Y.geojson
ga_ls8c_ard_3_2015--P6Y-2020--P1Y.geojson  
ga_ls8c_ard_3_2015--P6Y.db
ga_ls8c_ard_3_2015--P6Y.csv
ga_ls8c_nbart_gm_cyear_3.db
```

The csv file contains the list of tasks for all the years and consists of x, y coordinates of each tile to be processed as well as the counts of datasets and satellite observations for each tile.

Geojson files are useful when selecting test regions as well as for debugging specific tasks - see the example below from Landsat-8, 2015.  This example shows a tile from Australia coastal region near Sydney.

<img src="odc/stats/stats/auxiliary/screenshot-L8-2015.png" alt="drawing" width="1000"/>


### 3- Run stats
Sample command:
```
odc-stats run s3-path/ga_ls8c_nbart_gm_cyear_3.db  2019--P1Y/31/24 --threads=96 --memory-limit=700Gi --max-procesing-time 3600 --config cfg.txt --location file://localpath/
```  

Where cfg.txt contains the following configurations (sample from a geomedian run):
```
plugin: gm-s2
max_processing_time: 3600
job_queue_max_lease: 300
renew_safety_margin: 60
future_poll_interval: 2
s3_acl: bucket-owner-full-control
cog_opts:
    zlevel: 6
    overrides:
    rgba:
        compress: JPEG             
        jpeg_quality: 90                     
# Generic product attributes
product:
    name: gm_s2_annual
    short_name: gm_s2_annual
    version: 1.0.0
    product_family: geomedian
    collections_site: explorer.digitalearth.africa
    producer: digitalearthafrica.org
    region_code_format: "x{x:03d}y{y:03d}"
```

Note that the configurations vary between different products.  Samples of plugins configurations for Africa can be found in the following location. 
```
https://bitbucket.org/geoscienceaustralia/datakube-apps/src/master/workspaces/deafrica-prod-af/processing/
```

Orchestration
-----

In order to run stats on a large area, use stats orchestration tool which relies on SQS queues and Kubernetes jobs.  Before running a job, one need to create infrastructures using below step.

### 1- Create queues and a user 
Each user will need to create a queue to publish their tasks to.  Each queue will have a corresponding dead letter queue. 

The user and the queue will be created using a terraform module which contains code similar to the following:

```
module "odc_stats_geomedian" {

  source = "../../../modules/statistician"

  stats_process = "geomedian"

  destination_bucket = "destination-bucket-name"
  destination_path   = "gm_s2_annual"

  test_bucket = "test-bucket-name"

  cluster_id = local.cluster_id
  owner      = local.owner
}
```
The stats process name is used to name the user and the queue, and the destination and test buckets specifies the output locations in which this user has access to.

for an example, check the following file:
```
https://bitbucket.org/geoscienceaustralia/datakube/src/master/workspaces/deafrica-dev/03_odc_k8s_apps/stats_procesing.tf
```

When you run stats using jobs, tasks that are tried three time and have failed, will end up in the dead letter queue.  The user can then find the logs corresponding to these tasks and identify why they failed.  Once underlying issues are detected (bad datasets for example), a new cache file can be generated with ```save-tasks``` and the tasks can be rerun.  If tasks have failed for resource issues, usually rerun will pass.


### 2- Publish tasks 

```
odc-stats publish-tasks S3-bucket/S3-prefix/ga_ls8c_nbart_gm_cyear_3.db  stats-queue [task-id/index] --dry-run
```
This function populates  ```stats-queue``` from the cache file. Each message contains a tile id or an index. An example of a tile id is shown below:

```x+003/y+004/2019--P1Y```  

```publish-tasks``` publishes all the messages in the cache file by default.  This command offers an option to publish a subset of tasks based on their indexes from the csv file.  For example, if your csv file contains tasks across multiple years, you can just publish tasks belonging to a particular year by providing the range:

```
tasks=$(shuf -i  1000-2000)
```

where 1000 to 2000 covers the indexes for a single year.
Alway run with the dryrun option and check tasks against the csv file to ensure you're publishing the correct set of indexes.

### 3- Run tasks 

in order to run a job, create a job template.  The hob template is a yaml file which specifies your run configuration.  This includes, location of the cache file containing the status of the dataset, a queue containing the list of tasks to process, AWS node group types to be deployed, resource requests and limits, plugin and product configs.  An example of the job definition for running geometric median on Landsat-8 can be found in the following location.


<span style="color:red">WARNING:  Do not run the following configurations directly as they may overwrite existing data.  Always change the output directory and create your own infrastructure by running the step 1 above before running stats.</span>
```
https://bitbucket.org/geoscienceaustralia/datakube-apps/src/develop/workspaces/dea-dev/processing/06_stats.yaml
```
### Issues 

During the run, monitor the number of messages inflight, this can be done from cli or the dashboard.  Counts of inflight messages should be the same as (or close to) the number of pod/replicas.

Monitor the AWS Autoscaling Groups and keep track of the number of instances that it successfully obtains during the run.  This should match the number of the replicas that you've defined in your job template.

Currently when the jobs complete, pods do not shutdown cleanly due to an error in the dask library.  In order to ensure assigned EC2 instances are released, you need to delete the job manually by running the following command:

``` 
kp delete -f path-to-job-template.yaml
```
### 4- Redrive queue
For tasks that have failed and need to be retried, they be redirected to the main stats queue using the following command:

```
redrive-queue stats-dead-letter-queue-name  dead-letter-queue-name 
```

### Monitoring
Grafana is a powerful to monitor K8 jobs and pods.  You can query all the logs for a single job, to detect errors, or drill down into logs of a particular pod to identify specif issues for tasks.  To use this, log into Grafana corresponding to the cluster that you're running the job in,from the left panel, select ```Explore```, change the explore method to ```Loki```, from log label, select ```job_name``` and then select your job name for example ```stats-geomedian```.  Youcan now run a query on the log of the selected job:

<img src="odc/stats/stats/auxiliary/screenshot-grafana.png" alt="drawing" width="1000"/>

