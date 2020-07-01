import click
import sys
from odc.aio import s3_find_glob, S3Fetcher


@click.command('s3-find')
@click.option('--skip-check', is_flag=True,
              help='Assume file exists when listing exact file rather than wildcard.')
@click.option('--aws-unsigned', is_flag=True,
              help='Do not sign AWS S3 requests')
@click.argument('uri', type=str, nargs=1)
def cli(uri, skip_check, aws_unsigned=None):
    """ List files on S3 bucket.

    Example:

       \b
       List files in directory that match `*yaml`
        > s3-find 's3://mybucket/some/path/*yaml'

       \b
       List files in directory and all sub-directories that match `*yaml`
        > s3-find 's3://mybucket/some/path/**/*yaml'

       \b
       List files that match `*yaml` 2 levels deep from known path
        > s3-find 's3://mybucket/some/path/*/*/*yaml'

       \b
       List directories 2 levels deep from known path
        > s3-find 's3://mybucket/some/path/*/*/'

       \b
       List all files named `metadata.yaml` 2 directories deep
        > s3-find 's3://mybucket/some/path/*/*/metadata.yaml'
    """
    flush_freq = 100

    s3 = S3Fetcher(aws_unsigned=aws_unsigned)

    try:
        stream = s3_find_glob(uri, skip_check=skip_check, s3=s3)
        for i, o in enumerate(stream):
            print(o.url, flush=(i % flush_freq == 0))
    except ValueError as ve:
        click.echo(str(ve), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(str(e), err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()
