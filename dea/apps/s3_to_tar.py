import click
from collections import namedtuple
import queue
from threading import Thread
import io
import tarfile
import time

from dea.aws.s3async import fetch_bunch
from dea.ppr import qmap
from dea.io import read_stdin_lines


Data = namedtuple('Data', 'url data idx time'.split(' '))


def add_txt_file(tar, fname, content, mode=0o644):
    info = tarfile.TarInfo(name=fname)
    if isinstance(content, str):
        content = content.encode('utf-8')
    info.size = len(content)
    info.mtime = time.time()  # TODO: get time from S3 object
    info.mode = mode
    tar.addfile(tarinfo=info, fileobj=io.BytesIO(content))


@click.command('s3-to-tar')
@click.option('-n', type=int,
              help='Number of concurrent async connections to S3')
@click.argument("outfile", type=str, nargs=1)
def cli(n, outfile):
    """ Fetch a bunch of s3 files into a tar archive.

    \b
    For every non-empty line in stdin
       - Treat line as a URI and fetch document from it
       - Write content of the file to a tar archive using `bucket-name/path/to/file` as file name
    """
    from sys import stderr

    nconnections = 64 if n is None else n

    q_raw = queue.Queue(maxsize=10_000)

    EOS = object()

    def on_data(data, url, idx=None, time=None):
        q_raw.put(Data(url, data, idx, time))

    def read_stage(urls):
        fetch_bunch(urls, on_data, nconnections=nconnections)
        q_raw.put(EOS)

    def dump_to_tar(data_stream):
        with tarfile.open(outfile, 'w') as tar:
            for d in data_stream:
                fname = d.url[4:]

                print(fname, len(d.data), file=stderr)
                add_txt_file(tar, fname, d.data)

    threads = []

    def launch(proc, *args, **kwargs):
        thread = Thread(target=proc, args=args, kwargs=kwargs)
        thread.start()
        threads.append(thread)

    urls = read_stdin_lines(skip_empty=True)
    launch(read_stage, urls)

    dump_to_tar(qmap(lambda x: x, q_raw, eos_marker=EOS))

    for th in threads:
        th.join()


if __name__ == '__main__':
    cli()
