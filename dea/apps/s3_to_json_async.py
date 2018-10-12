import yaml
import json
import click
from collections import namedtuple
import queue
from threading import Thread

from dea.aws.s3async import fetch_bunch
from dea.ppr import q2q_map, qmap
from dea.io import read_stdin_lines


Data = namedtuple('Data', 'url data idx time'.split(' '))


def parse_yaml(data):
    try:
        return yaml.load(data, Loader=yaml.CSafeLoader), None
    except Exception as e:
        return None, str(e)


def process_doc(url, data):
    metadata, error = parse_yaml(data)
    if metadata is None:
        return None, error

    out = dict(metadata=metadata,
               uris=[url])
    try:
        out = json.dumps(out, separators=(',', ':'), check_circular=False)
    except Exception as e:
        return None, str(e)

    return out, None


def d2json(d):
    from sys import stderr

    out, err = process_doc(d.url, d.data)
    if err is not None:
        print('Failed: %s\n%s' % (d.url, err), file=stderr)
    return out


@click.command('s3-to-json-async')
@click.option('-n', type=int,
              help='Number of concurrent async connections to S3')
@click.option('--threads', type=int,
              help='Number of yaml parsing threads')
def cli(n, threads):
    """ Turn s3 urls pointing to YAML files to JSON strings.

    \b
    For every non-empty line in stdin
       - Treat line as a URI and fetch YAML document from it
       - Generate JSON object with fields:
         - metadata -- contents of the YAML (parsed into object tree)
         - uris     -- list containing single uri from which `metadata` was fetched
       - Serialise JSON object to a single line in stdout
    """
    from sys import stderr

    n_worker_threads = 4 if threads is None else threads
    nconnections = 64 if n is None else n

    q_raw = queue.Queue(maxsize=10_000)
    q_json = queue.Queue(maxsize=10_000)

    EOS = object()

    def on_data(data, url, idx=None, time=None):
        q_raw.put(Data(url, data, idx, time))

    def read_stage(urls):
        fetch_bunch(urls, on_data, nconnections=nconnections)

        for _ in range(n_worker_threads):
            q_raw.put(EOS)

    def dump_to_stdout(lines):
        for i, l in enumerate(lines):
            print(l, flush=((i % 10) == 0))
            if (i % 100) == 0:
                print('{:9,d}'.format(i), file=stderr, end='\r', flush=True)

    threads = []

    def launch(proc, *args, **kwargs):
        thread = Thread(target=proc, args=args, kwargs=kwargs)
        thread.start()
        threads.append(thread)

    for _ in range(n_worker_threads):
        launch(q2q_map, d2json, q_raw, q_json, eos_marker=EOS)

    urls = read_stdin_lines(skip_empty=True)
    launch(read_stage, urls)

    dump_to_stdout(qmap(lambda x: x, q_json, eos_marker=EOS))

    for th in threads:
        th.join()


if __name__ == '__main__':
    cli()
