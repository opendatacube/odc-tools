import click
import sys
from types import SimpleNamespace
from dea.aws.aio import S3Fetcher
from dea.aws import parse_query, norm_predicate
from dea.ppr import future_results


@click.command('s3-find')
@click.option('--skip-check', is_flag=True,
              help='Assume file exists when listing exact file rather than wildcard.')
@click.argument('uri', type=str, nargs=1)
def cli(uri, skip_check):
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

    def do_file_query(qq, pred):
        for d in s3.dir_dir(qq.base, qq.depth):
            _, _files = s3.list_dir(d).result()
            for f in _files:
                if pred(f):
                    yield f

    def do_file_query2(qq):
        fname = qq.file

        stream = s3.dir_dir(qq.base, qq.depth)

        if skip_check:
            yield from (SimpleNamespace(url=d+fname) for d in stream)
            return

        stream = (s3.head_object(d+fname) for d in stream)

        for (f, _), _ in future_results(stream, 32):
            if f is not None:
                yield f

    def do_dir_query(qq):
        return (SimpleNamespace(url=url) for url in s3.dir_dir(qq.base, qq.depth))

    flush_freq = 100

    try:
        qq = parse_query(uri)
    except ValueError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    s3 = S3Fetcher()

    glob_or_file = qq.glob or qq.file

    if qq.depth is None and glob_or_file is None:
        stream = s3.find(qq.base)
    elif qq.depth is None or qq.depth < 0:
        if qq.glob:
            stream = s3.find(qq.base, glob=qq.glob)
        elif qq.file:
            postfix = '/'+qq.file
            stream = s3.find(qq.base, pred=lambda o: o.url.endswith(postfix))
    else:
        # fixed depth query
        if qq.glob is not None:
            pred = norm_predicate(glob=qq.glob)
            stream = do_file_query(qq, pred)
        elif qq.file is not None:
            stream = do_file_query2(qq)
        else:
            stream = do_dir_query(qq)

    for i, o in enumerate(stream):
        print(o.url, flush=(i % flush_freq == 0))


if __name__ == '__main__':
    cli()
