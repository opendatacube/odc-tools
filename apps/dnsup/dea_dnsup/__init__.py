import sys
from odc.aws.dns import cli as dns_cli


def cli():
    sys.exit(dns_cli(sys.argv[1:]))
