import tarfile
import click
import requests
from odc.io.tar import tar_mode, add_txt_file
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial

from urllib.parse import urlparse
from thredds_crawler.crawl import Crawl


def download(url):
    parsed_uri = urlparse(url)
    target_filename = url[len(parsed_uri.scheme + '://'):]

    return requests.get(url).content, target_filename


@click.command('thredds-to-tar')
@click.option('--thredds_catalogue', '-c', type=str, required=True, help="The THREDDS catalogue endpoint")
@click.option('--skips', '-s', type=str, multiple=True,
              help="Pattern to ignore when THREDDS crawling")
@click.option('--select', '-t', type=str, required=True,
              help="Target file pattern to match for yaml")
@click.option('--workers', '-w', type=int, default=4, help="Number of thredds crawler workers to use")
@click.option('--outfile', type=str, default="metadata.tar.gz", help="Sets the output file name")
def cli(thredds_catalogue,
        skips,
        select,
        workers,
        outfile):
    """ Download Metadata from THREDDS server to tarball

    Example:

       \b
       Download files in directory that match `*yaml` and store them as a tar
        > thredds-to-tar -c "http://dapds00.nci.org.au/thredds/catalog/if87/2018-11-29/"
        -t ".*ARD-METADATA.yaml" -s '.*NBAR.*' -s '.*SUPPLEMENTARY.*'
         -s '.*NBART.*' -s '.*/QA/.*' -w 8 --outfile 2018-11-29.tar.gz

    """

    user_skips = Crawl.SKIPS
    for skip in skips:
        user_skips = user_skips+[skip]

    print("Searching {thredds_catalogue} for matching files".format(thredds_catalogue=thredds_catalogue))
    results = Crawl(thredds_catalogue + '/catalog.xml', select=[select], skip=user_skips, workers=workers).datasets

    print("Found {0} metadata files".format(str(len(results))))

    urls = [service['url'] for dataset in results
            for service in dataset.services
            if service['service'].lower() == 'httpserver']

    # use a threadpool to download from thredds
    pool = ThreadPool(workers)
    yamls = pool.map(partial(download), urls)
    pool.close()
    pool.join()

    # jam it all in a tar
    tar_opts = dict(name=outfile, mode='w' + tar_mode(gzip=True, xz=True, is_pipe=False))
    with tarfile.open(**tar_opts) as tar:
        for yaml in yamls:
            add_txt_file(tar=tar, content=yaml[0], fname=yaml[1])

    print("Done!")


if __name__ == '__main__':
    cli()
