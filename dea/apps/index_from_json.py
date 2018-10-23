import click
import sys
import toolz
import json
import datacube
from datacube.index.hl import Doc2Dataset


def from_json_lines(lines, index, **kwargs):
    doc2ds = Doc2Dataset(index, **kwargs)

    def clean_stream(lines):
        for lineno, line in enumerate(lines):
            line = line.strip()

            if len(line) > 0:
                yield lineno, line

    for lineno, l in clean_stream(lines):
        try:
            doc = json.loads(l)
        except json.JSONDecodeError as e:
            print('Error[%d]: %s' % (lineno, str(e)))
            continue

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
@click.argument('input_fname', type=str, nargs=-1)
def cli(input_fname, env=None):

    def process_file(f, index):
        n_total = 0
        n_failed = 0

        for ds in from_json_lines(f, index, verify_lineage=False):
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
        if filename == '-':
            process_file(sys.stdin, dc.index)
        else:
            with open(filename, 'rt') as f:
                process_file(f, dc.index)


if __name__ == '__main__':
    cli()
