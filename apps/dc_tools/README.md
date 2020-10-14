# odc.apps.dc_tools

Command line utilities for working with datacube index

## Installation

``` bash
pip install --extra-index-url="https://packages.dea.ga.gov.au" odc_apps_dc_tools
```

## Usage

### dc-index-export-md

Metadata transformer

Simple usage:

``` bash
TODO:

```

Extended usage:

``` bash
TODO:
```

### dc-index-from-tar

Index ODC metadata that is contained in a .tar file

Simple usage:

``` bash
dc-index-from-tar 'path/to/file.tar'

```

Extended usage:

``` bash
TODO:
```

### sqs-to-dc

A tool to index from an SQS queue

Simple usage:

``` bash
sqs-to-dc example-queue-name 'product-name-a product-name-b'

```

Extended usage:

``` bash
TODO:
```

### s3-to-dc

A tool for indexing from S3.

Simple usage:

``` bash
s3-to-dc 's3://bucket/path/**/*.yaml' 'product-name-a product-name-b'

```

Extended usage:

``` bash
TODO:
```

### thredds-to-dc

Index from a THREDDS server

Simple usage:

``` bash
TODO:

```

Extended usage:

``` bash
TODO:
```
