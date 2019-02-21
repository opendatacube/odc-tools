import os
import tarfile
import click
import shutil
import urllib.request
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial

from urllib.parse import urlparse
from thredds_crawler.crawl import Crawl


def download(result, parsed_uri, file_retriever):
    thredds_id = result.id

    source_filename = parsed_uri.geturl() + thredds_id
    target_filename = source_filename[len(parsed_uri.scheme + '://'):]
    # ensure dir exists
    if not os.path.exists(os.path.dirname('./' + target_filename)):
        os.makedirs(os.path.dirname('./' + target_filename))

    # download to tar
    file_retriever.retrieve(source_filename, target_filename)

    return target_filename


@click.command('thredds-to-tar')
@click.option('--thredds_catalogue', '-c', type=str, required=True, help="The THREDDS catalogue endpoint")
@click.option('--thredds_fileserver', '-f', type=str, required=True, help="The THREDDS HTTP fileserver endpoint")
@click.option('--skips', '-s', type=str, multiple=True,
              help="Pattern to ignore when THREDDS crawling")
@click.option('--select', '-t', type=str, required=True,
              help="Target file pattern to match for yaml")
@click.option('--workers', '-w', type=int, default=4, help="Number of thredds crawler workers to use")
@click.option('--outfile', type=str, default="metadata.tar.gz", help="Sets the output file name")
def cli(thredds_catalogue,
        thredds_fileserver,
        skips,
        select,
        workers,
        outfile):
    """ Download Metadata from THREDDS server to tarball

    Example:

       \b
       Download files in directory that match `*yaml` and store them as a tar
        > thredds-to-tar -c "http://dapds00.nci.org.au/thredds/catalog/if87/2018-11-29/"
        -f "http://dapds00.nci.org.au/thredds/fileServer/" -t ".*ARD-METADATA.yaml" -s '.*NBAR.*' -s '.*SUPPLEMENTARY.*'
         -s '.*NBART.*' -s '.*/QA/.*' -w 8 --outfile 2018-11-29.tar.gz

    """
    file_retriever = urllib.request.URLopener()

    user_skips = Crawl.SKIPS
    for skip in skips:
        user_skips = user_skips+[skip]

    print("Searching {thredds_catalogue} for matching files".format(thredds_catalogue=thredds_catalogue))
    results = Crawl(thredds_catalogue + '/catalog.xml', select=[select], skip=user_skips, workers=workers).datasets

    file_count = str(len(results))
    print("Found {0} metadata files".format(file_count))

    # parse fileserver url for protocol and domain to delete later
    parsed_uri = urlparse(thredds_fileserver)

    pool = ThreadPool(workers)
    downloaded_files = pool.map(partial(download, parsed_uri=parsed_uri, file_retriever=file_retriever), results)
    pool.close()
    pool.join()

    tar = tarfile.open(outfile, "w:gz")
    for downloaded_file in downloaded_files:
        tar.add(downloaded_file)

        # remove
        os.remove('./' + downloaded_file)

    tar.close()

    shutil.rmtree("./" + parsed_uri.netloc)

    print("Done!")


if __name__ == '__main__':
    cli()
