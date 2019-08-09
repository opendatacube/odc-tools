odc.apps.dnsup
==============

Command line utility to update DNS on route53 from EC2 instance


Installation
------------

```
pip install --extra-index-url="https://packages.dea.gadevs.ga" dea_dnsup
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

For this to work, EC2 instance should have permissions to update DNS record in
the right zone. For DEA when using `{something}.devbox.dea.ga.gov.au` the policy
is `devbox-route53`.
