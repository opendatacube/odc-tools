import click
import datetime
import io
import tarfile
import time
import logging
import signal

from dea.aws.aio import S3Fetcher
from dea.io import read_stdin_lines
from dea.io.tar import tar_mode
from dea.bench import RateEstimator


def add_txt_file(tar, fname, content, mode=0o644, last_modified=None):
    if last_modified is None:
        last_modified = time.time()

    if isinstance(last_modified, datetime.datetime):
        last_modified = last_modified.timestamp()

    info = tarfile.TarInfo(name=fname)
    if isinstance(content, str):
        content = content.encode('utf-8')
    info.size = len(content)
    info.mtime = last_modified
    info.mode = mode
    tar.addfile(tarinfo=info, fileobj=io.BytesIO(content))


@click.command('s3-to-tar')
@click.option('-n', type=int,
              help='Number of concurrent async connections to S3')
@click.option('--io-threads', type=int, default=1,
              help='Number of IO threads (experimental)')
@click.option('--verbose', '-v', is_flag=True, help='Be verbose')
@click.option('--gzip', is_flag=True, help='Compress with gzip')
@click.option('--xz', is_flag=True, help='Compress with xz')
@click.argument("outfile", type=str, nargs=1, default='-')
def cli(n, io_threads, verbose, gzip, xz, outfile):
    """ Fetch a bunch of s3 files into a tar archive.

    \b
    For every non-empty line in stdin
       - Treat line as a URI and fetch document from it
       - Write content of the file to a tar archive using `bucket-name/path/to/file` as file name
    """
    import sys
    from sys import stderr, stdout
    logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=logging.ERROR)

    nconnections = 24 if n is None else n
    exit_early = False

    def dump_to_tar(data_stream, tar):
        nonlocal exit_early
        fps = RateEstimator()

        for d in data_stream:
            fps()
            fname = d.url[5:]

            if d.data is not None:
                if verbose:
                    if fps.every(10):
                        print('.', file=stderr, end='', flush=True)

                    if fps.every(100):
                        print(' {}'.format(str(fps)), file=stderr)

                add_txt_file(tar, fname, d.data, last_modified=d.last_modified)
            else:
                print("Failed %s (%s)" % (d.url, str(d.error)),
                      file=stderr)

            if exit_early:
                break

        if verbose:
            print(' {}'.format(str(fps)), file=stderr)

    fetcher = S3Fetcher(nthreads=io_threads,
                        nconcurrent=nconnections,
                        max_buffer=10_000)
    is_pipe = outfile == '-'
    tar_opts = dict(mode='w'+tar_mode(gzip=gzip, xz=xz, is_pipe=is_pipe))
    if is_pipe:
        if stdout.isatty():
            click.echo("Will not write to a terminal", err=True)
            sys.exit(1)
        # TODO: on windows switch stdout to binary mode
        tar_opts['fileobj'] = stdout.buffer
    else:
        tar_opts['name'] = outfile

    urls = read_stdin_lines(skip_empty=True)

    def on_ctrlc(sig, frame):
        nonlocal exit_early
        print('Shuttting down', file=sys.stderr)
        exit_early = True

    signal.signal(signal.SIGINT, on_ctrlc)

    with tarfile.open(**tar_opts) as tar:
        dump_to_tar(fetcher(urls), tar)

    fetcher.close()


if __name__ == '__main__':
    cli()
