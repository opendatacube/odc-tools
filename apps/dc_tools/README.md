# odc.apps.dc_tools

Command line utilities for working with datacube index

## Installation

``` bash
pip install odc-apps-dc-tools
```

## Usage

### dc-sync-products

The tool `dc-sync-products` helps you by keeping a Datacube instance's list of products up to date
with a CSV list of product names and definitions.

Basic usage is:

``` bash
dc-sync-products <path-to-csv> --update-if-exists
```

The `--update-if-exists` flag is optional, and will update a product, including unsafe changes, if it already exists.
The format for the CSV is as follows (note that you can have multiple products defined in one file):

```
product,definition
dem_srtm,https://raw.githubusercontent.com/digitalearthafrica/config/master/products/dem_srtm.odc-product.yaml
ls5_c2l2_sr;ls7_c2l2_sr;ls8_c2l2_sr;ls9_c2l2_sr,https://raw.githubusercontent.com/opendatacube/datacube-dataset-config/main/products/lsX_c2l2_sr.odc-product.yaml

```

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

``` text
Usage: sqs-to-dc [OPTIONS] QUEUE_NAME PRODUCT

  Iterate through messages on an SQS queue and add them to datacube

Options:
  --skip-lineage                  Default is not to skip lineage. Set to skip
                                  lineage altogether.

  --fail-on-missing-lineage / --auto-add-lineage
                                  Default is to fail if lineage documents not
                                  present in the database. Set auto add to try
                                  to index lineage documents.

  --verify-lineage                Default is no verification. Set to verify
                                  parent dataset definitions.

  --stac                          Expect STAC 1.0 metadata and attempt to
                                  transform to ODC EO3 metadata

  --odc-metadata-link TEXT        Expect metadata doc with ODC EO3 metadata
                                  link. Either provide '/' separated path to
                                  find metadata link in a provided metadata
                                  doc e.g. 'foo/bar/link', or if metadata doc
                                  is STAC, provide 'rel' value of the 'links'
                                  object having metadata link. e.g. 'STAC-
                                  LINKS-REL:odc_yaml'

  --limit INTEGER                 Stop indexing after n datasets have been
                                  indexed.

  --update                        If set, update instead of add datasets
  --update-if-exists              If the dataset already exists, update it
                                  instead of skipping it.

  --archive                       If set, archive datasets
  --allow-unsafe                  Allow unsafe changes to a dataset. Take
                                  care!

  --record-path TEXT              Filtering option for s3 path, i.e.
                                  'L2/sentinel-2-nrt/S2MSIARD/*/*/ARD-
                                  METADATA.yaml'

  --region-code-list-uri TEXT     A path to a list (one item per line, in txt
                                  or gzip format) of valide region_codes to
                                  include

  --absolute                      Use absolute paths when converting from stac

  --archive-less-mature           Find less mature versions of the dataset and
                                  archive them
                                  
  --publish-action SNS ARN        Publish indexing action to SNS topic

  --help                          Show this message and exit.
```

### s3-to-dc

A tool for indexing from S3.

Simple usage:

``` bash
s3-to-dc 's3://bucket/path/**/*.yaml' 'product-name-a product-name-b'

```

Extended usage:

The following command updates the datasets instead of adding them and allows unsafe changes. Be careful!

``` text
Usage: s3-to-dc [OPTIONS] URI PRODUCT

  Iterate through files in an S3 bucket and add them to datacube

Options:
  --skip-lineage                  Default is not to skip lineage. Set to skip
                                  lineage altogether.

  --fail-on-missing-lineage / --auto-add-lineage
                                  Default is to fail if lineage documents not
                                  present in the database. Set auto add to try
                                  to index lineage documents.

  --verify-lineage                Default is no verification. Set to verify
                                  parent dataset definitions.

  --stac                          Expect STAC 1.0 metadata and attempt to
                                  transform to ODC EO3 metadata

  --update                        If set, update instead of add datasets
  --update-if-exists              If the dataset already exists, update it
                                  instead of skipping it.

  --allow-unsafe                  Allow unsafe changes to a dataset. Take
                                  care!

  --skip-check                    Assume file exists when listing exact file
                                  rather than wildcard.

  --no-sign-request               Do not sign AWS S3 requests
  --request-payer                 Needed when accessing requester pays public
                                  buckets

  --archive-less-mature           Find less mature versions of the dataset and
                                  archive them

  --publish-action SNS ARN        Publish indexing action to SNS topic

  --help                          Show this message and exit.
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

### esri-lc-to-dc

Removed, use the `stac-to-dc` tool instead.

``` bash
  stac-to-dc \
    --catalog-href=https://planetarycomputer.microsoft.com/api/stac/v1/ \
    --collections='io-lulc'
```
