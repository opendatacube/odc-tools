import logging
import signal
import sys
import tarfile
from sys import stderr, stdout

import click
from odc.aio import S3Fetcher
from odc.io import read_stdin_lines
from odc.io.tar import add_txt_file, tar_mode
from odc.io.timer import RateEstimator


@click.command("s3-to-tar")
@click.option("-n", type=int, help="Number of concurrent async connections to S3")
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
@click.option("--gzip", is_flag=True, help="Compress with gzip")
@click.option("--xz", is_flag=True, help="Compress with xz")
@click.option("--no-sign-request", is_flag=True, help="Do not sign AWS S3 requests")
@click.option(
    "--request-payer",
    is_flag=True,
    help="Needed when accessing requester pays public buckets",
)
@click.argument("outfile", type=str, nargs=1, default="-")
def cli(n, verbose, gzip, xz, outfile, no_sign_request=None, request_payer=False):
    """Fetch a bunch of s3 files into a tar archive.

    \b
    For every non-empty line in stdin
       - Treat line as a URI and fetch document from it
       - Write content of the file to a tar archive using `bucket-name/path/to/file` as file name
    """

    opts = {}
    if request_payer:
        opts["RequestPayer"] = "requester"

    logging.basicConfig(
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        level=logging.ERROR,
    )

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
                        print(".", file=stderr, end="", flush=True)

                    if fps.every(100):
                        print(" {}".format(str(fps)), file=stderr)

                add_txt_file(tar, fname, d.data, last_modified=d.last_modified)
            else:
                print("Failed %s (%s)" % (d.url, str(d.error)), file=stderr)

            if exit_early:
                break

        if verbose:
            print(" {}".format(str(fps)), file=stderr)

    fetcher = S3Fetcher(nconcurrent=nconnections, aws_unsigned=no_sign_request)
    is_pipe = outfile == "-"
    tar_opts = dict(mode="w" + tar_mode(gzip=gzip, xz=xz, is_pipe=is_pipe))
    if is_pipe:
        if stdout.isatty():
            click.echo("Will not write to a terminal", err=True)
            sys.exit(1)
        # TODO: on windows switch stdout to binary mode
        tar_opts["fileobj"] = stdout.buffer
    else:
        tar_opts["name"] = outfile

    urls = read_stdin_lines(skip_empty=True)

    def on_ctrlc(sig, frame):
        nonlocal exit_early
        print("Shutting down...", file=sys.stderr)
        exit_early = True

    signal.signal(signal.SIGINT, on_ctrlc)

    with tarfile.open(**tar_opts) as tar:
        dump_to_tar(fetcher(urls, **opts), tar)

    fetcher.close()


if __name__ == "__main__":
    cli()
