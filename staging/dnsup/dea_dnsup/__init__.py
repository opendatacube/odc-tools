import sys

from odc.aws.dns import cli as dns_cli

from ._version import __version__


def cli():
    sys.exit(dns_cli(sys.argv[1:]))
