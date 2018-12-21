import re
from fnmatch import fnmatch
import sys
import click
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory


def build_predicate(glob=None, regex=None, prefix=None):
    def match_prefix(entry):
        return entry.Key.startswith(prefix)

    def match_regex(entry):
        return bool(regex.match(entry.Key))

    def match_glob(entry):
        return fnmatch(entry.Key, glob)

    preds = []

    if prefix:
        prefix = prefix.lstrip('/')
        preds.append(match_prefix)

    if regex is not None:
        regex = re.compile(regex)
        preds.append(match_regex)

    if glob is not None:
        preds.append(match_glob)

    if len(preds) == 0:
        return lambda x: True
    elif len(preds) == 1:
        return preds[0]
    elif len(preds) == 2:
        p1, p2 = preds
        return lambda e: p1(e) and p2(e)
    else:
        raise ValueError('regex and glob are mutually exclusive')


@click.command('s3-inventory-dump')
@click.option('--inventory', '-i', type=str, help='URL pointing to manifest.json or one level up')
@click.option('--prefix', type=str, help='Only print entries with Key starting with `prefix`')
@click.option('--regex', type=str, help='Only print entries matching regex')
@click.option('--aws-profile', type=str, help='Use non-default aws profile')
@click.argument('glob', type=str, default='', nargs=1)
def cli(inventory, prefix, regex, glob, aws_profile):
    """List S3 inventory entries.

        prefix can be combined with regex or glob pattern, but supplying both
        regex and glob doesn't make sense.

    \b
    Example:
       s3-inventory s3://my-inventory-bucket/path-to-inventory/ '*yaml'

    """

    def entry_to_url(entry):
        return 's3://{e.Bucket}/{e.Key}'.format(e=entry)

    flush_freq = 100
    s3 = make_s3_client(profile=aws_profile)

    if glob == '':
        glob = None

    if glob is not None and regex is not None:
        click.echo("Can not mix regex and shell patterns")
        sys.exit(1)

    if inventory is None:
        # TODO: read from config file
        inventory = 's3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/'

    predicate = build_predicate(glob=glob, regex=regex, prefix=prefix)

    to_str = entry_to_url

    for i, entry in enumerate(list_inventory(inventory, s3=s3)):
        if predicate(entry):
            print(to_str(entry), flush=(i % flush_freq) == 0)


if __name__ == '__main__':
    cli()
