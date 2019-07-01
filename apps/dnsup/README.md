odc.apps.dnsup
==============

Command line utility to update DNS on route53 from EC2 instance


Installation
------------

```
pip install 'git+https://github.com/opendatacube/dea-proto.git#egg=odc_apps_dnsup&subdirectory=apps/dnsup'
```

Usage
-----

Run `dea-dnsup --help`

```
Modify DNS record of EC2 instance:

arguments: domain_name|tag/<tag name containing domain name> [auto|delete|ip]

Examples:
  test.devbox.dea.ga.gov.au
  test.devbox.dea.ga.gov.au auto
  test.devbox.dea.ga.gov.au 3.44.10.22
  tag/domain auto
  tag/domain delete
```
