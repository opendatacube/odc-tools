import click
import sys
from dea.aws.aio import S3Fetcher
from dea.aws._find import parse_query, norm_predicate


@click.command('s3-find')
@click.argument('uri', type=str, nargs=1)
def cli(uri):
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
    """

    def do_query(qq, pred):
        for d in s3.dir_dir(qq.base, qq.depth):
            _, _files = s3.list_dir(d).result()
            for f in _files:
                if pred(f):
                    yield f

    flush_freq = 100

    try:
        qq = parse_query(uri)
    except ValueError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    s3 = S3Fetcher()

    glob = qq.glob or qq.file

    if qq.depth < 0 or (qq.glob is None and qq.file is None):
        stream = s3.find(qq.base, glob=glob)
    else:
        # fixed depth query
        pred = norm_predicate(glob=glob)
        stream = do_query(qq, pred)

    for i, o in enumerate(stream):
        print(o.url, flush=(i % flush_freq == 0))


if __name__ == '__main__':
    cli()
