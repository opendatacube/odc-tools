odc.stats
=========

Statistical product generation framework.
WORK IN PROGRESS

Installation
------------

```
pip install --extra-index-url="https://packages.dea.ga.gov.au" odc-stats
```

Usage
-----

### Prepare tasks for processing:

From a machine that has access to your database, run:

```
odc-stats save-tasks --year YYYY --grid  grid-name  --temporal-range temporal-range product-name
```
This will generate a cache file that contains tasks.  For more options, check the help.  
### Publish tasks 

```
odc-stats publish-tasks DB-file-path queue-name 
```
This function populates the queue from a cache file. Each message contains a tile id. An example of a tile id is shown below:

```x+003/y-004/2019--P1Y```  

