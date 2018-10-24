import click
import sys
import datacube
from dea.io import tar_doc_stream
from dea.bench import RateEstimator
from dea.index import from_yaml_doc_stream


def from_tar_file(tarfname, index, mk_uri, **kwargs):
    """ returns a sequence of tuples where each tuple is either

        (ds, None) or (None, error_message)
    """
    def untar(tarfname, mk_uri):
        for doc_name, doc in tar_doc_stream(tarfname):
            yield mk_uri(doc_name), doc

    return from_yaml_doc_stream(untar(tarfname, mk_uri), index, **kwargs)


@click.command('index_from_tar')
@click.option('--env', '-E', type=str, help='Datacube environment name')
@click.option('--verify-lineage/--no-verify-lineage', is_flag=True, default=True,
              help=('Lineage referenced in the metadata document should be the same as in DB, '
                    'default behaviour is to skip those top-level datasets that have lineage data '
                    'different from the version in the DB. This option allows omitting verification step.'))
@click.option('--ignore-lineage',
              help="Pretend that there is no lineage data in the datasets being indexed",
              is_flag=True, default=False)
@click.argument('input_fname', type=str, nargs=-1)
def cli(input_fname, env=None, verify_lineage=True, ignore_lineage=False):

    ds_resolve_args = dict(verify_lineage=verify_lineage,
                           skip_lineage=ignore_lineage)

    def mk_s3_uri(name):
        return 's3://' + name

    def report_error(msg):
        print(msg, file=sys.stderr)

    def process_file(filename, index, fps, n_failed=0):
        for ds, err in from_tar_file(filename, index, mk_s3_uri, **ds_resolve_args):
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
