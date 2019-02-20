import os
import tarfile
import click
import shutil
import urllib.request

from urllib.parse import urlparse
from thredds_crawler.crawl import Crawl


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

    # parse fileserver url for protocol and domain to delte later
    parsed_uri = urlparse(thredds_fileserver)

    # Download Files to tar with an updating counter
    count = 0
    tar = tarfile.open(outfile, "w:gz")
    for result in results:
        thredds_id = result.id

        source_filename = thredds_fileserver + thredds_id
        target_filename = source_filename[len(parsed_uri.scheme+'://'):]
        # ensure dir exists
        if not os.path.exists(os.path.dirname('./' + target_filename)):
            os.makedirs(os.path.dirname('./' + target_filename))

        # download to tar
        file_retriever.retrieve(source_filename, target_filename)
        tar.add(target_filename)

        # remove
        os.remove('./' + target_filename)

        count += 1
        # counter
        print("{count}/{file_count} Downloaded".format(count=count, file_count=file_count))

    tar.close()

    shutil.rmtree("./" + parsed_uri.netloc)

    print("Done!")


if __name__ == '__main__':
    cli()
