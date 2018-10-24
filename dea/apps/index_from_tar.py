import click
import sys
import datacube
from datacube.index.hl import Doc2Dataset
from dea.io import tar_doc_stream
from dea.io.text import parse_yaml
from dea.bench import RateEstimator


def from_tar_file(tarfname, index, mk_uri, **kwargs):
    """ returns a sequence of tuples where each tuple is either

        (ds, None) or (None, error_message)
    """
    doc2ds = Doc2Dataset(index, **kwargs)

    for doc_name, doc in tar_doc_stream(tarfname):
        try:
            metadata = parse_yaml(doc)
        except Exception as e:
            yield (None, 'Error: %s, %s' % (doc_name, str(e)))
            continue

        if metadata is None:
            yield (None, 'Error: failed to parse: %s, "%s"' % (doc_name, doc))
            continue

        uri = mk_uri(doc_name)

        ds, err = doc2ds(metadata, uri)
        if ds is not None:
            yield (ds, None)
        else:
            yield (None, 'Error: %s, %s' % (doc_name, err))


@click.command('index_from_tar')
@click.option('--env', type=str, help='Datacube environment name')
@click.argument('input_fname', type=str, nargs=-1)
def cli(input_fname, env=None):

    def mk_s3_uri(name):
        return 's3://' + name

    def report_error(msg):
        print(msg, file=sys.stderr)

    def process_file(filename, index, fps, n_failed=0):
        for ds, err in from_tar_file(filename, index, mk_s3_uri, verify_lineage=False):
            if ds is not None:
                try:
                    index.datasets.add(ds, with_lineage=True)
                except Exception as e:
                    n_failed += 1
                    report_error(str(e))
            else:
                n_failed += 1
                report_error(err)

            fps()

            if fps.every(10):
                print('.', end='', flush=True)

            if fps.every(100):
                print(' {} F:{:d}'.format(str(fps), n_failed))

        return n_failed

    dc = datacube.Datacube(env=env)

    if len(input_fname) == 0:
        input_fname = ('-',)

    n_failed = 0
    fps = RateEstimator()
    for filename in input_fname:
        n_failed = process_file(filename, dc.index, fps, n_failed)

    if n_failed > 0:
        report_error("**WARNING** there were failures: {}".format(n_failed))


if __name__ == '__main__':
    cli()
