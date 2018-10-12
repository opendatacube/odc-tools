import click
import toolz
import json
import datacube
from datacube.index.hl import Doc2Dataset


def from_json_lines(lines, index, **kwargs):
    doc2ds = Doc2Dataset(index, **kwargs)

    for lineno, l in enumerate(lines):
        try:
            doc = json.loads(l)
        except json.JSONDecodeError as e:
            print('Error[%d]: %s' % (lineno, str(e)))

        uri = toolz.get_in(['uris', 0], doc)
        if uri is None:
            print('Error[%d]: missing uri' % lineno)
            continue

        metadata = doc.get('metadata')
        if metadata is None:
            print('Error[%d]: missing metadata' % lineno)
            continue

        ds, err = doc2ds(metadata, uri)
        if ds is not None:
            yield ds
        else:
            print('Error[%d]: %s' % (lineno, err))


@click.command('index_from_json')
@click.option('--env', type=str, help='Datacube environment name')
@click.argument('input_fname', type=str, nargs=1)
def cli(input_fname, env=None):
    dc = datacube.Datacube(env=env)

    n_total = 0
    n_failed = 0

    with open(input_fname, 'rt') as f:
        for ds in from_json_lines(f, dc.index, verify_lineage=False):
            n_total += 1
            try:
                dc.index.datasets.add(ds, with_lineage=True)
            except Exception as e:
                n_failed += 1
                print(str(e))

            if (n_total % 10) == 0:
                print('.', end='', flush=True)

            if (n_total % 100) == 0:
                print(' T:{:d} F:{:d}'.format(n_total, n_failed))


if __name__ == '__main__':
    cli()
