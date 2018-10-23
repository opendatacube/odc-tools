import click
import datacube
from datacube.index.hl import Doc2Dataset
from dea.io import tar_doc_stream
from dea.io.text import parse_yaml


def from_tar_file(tarfname, index, mk_uri, **kwargs):
    doc2ds = Doc2Dataset(index, **kwargs)

    for doc_name, doc in tar_doc_stream(tarfname):
        try:
            metadata = parse_yaml(doc)
        except Exception as e:
            print('Error: %s, %s' % (doc_name, str(e)))
            continue

        if metadata is None:
            print('Error: failed to parse: %s, "%s"' % (doc_name, doc))
            continue

        uri = mk_uri(doc_name)

        ds, err = doc2ds(metadata, uri)
        if ds is not None:
            yield ds
        else:
            print('Error: %s, %s' % (doc_name, err))


@click.command('index_from_tar')
@click.option('--env', type=str, help='Datacube environment name')
@click.argument('input_fname', type=str, nargs=-1)
def cli(input_fname, env=None):

    def mk_s3_uri(name):
        return 's3://' + name

    def process_file(filename, index):
        n_total = 0
        n_failed = 0

        for ds in from_tar_file(filename, index, mk_s3_uri, verify_lineage=False):
            n_total += 1
            try:
                index.datasets.add(ds, with_lineage=True)
            except Exception as e:
                n_failed += 1
                print(str(e))

            if (n_total % 10) == 0:
                print('.', end='', flush=True)

            if (n_total % 100) == 0:
                print(' T:{:d} F:{:d}'.format(n_total, n_failed))

    dc = datacube.Datacube(env=env)

    if len(input_fname) == 0:
        input_fname = ('-',)

    for filename in input_fname:
        process_file(filename, dc.index)


if __name__ == '__main__':
    cli()
