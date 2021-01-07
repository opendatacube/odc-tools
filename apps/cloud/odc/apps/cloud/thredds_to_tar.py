import tarfile
import click
from odc.io.tar import tar_mode, add_txt_file
from odc.thredds import thredds_find_glob, download_yamls


@click.command("thredds-to-tar")
@click.option(
    "--thredds_catalogue",
    "-c",
    type=str,
    required=True,
    help="The THREDDS catalogue endpoint",
)
@click.option(
    "--skips",
    "-s",
    type=str,
    multiple=True,
    help="Pattern to ignore when THREDDS crawling",
)
@click.option(
    "--select",
    "-t",
    type=str,
    required=True,
    help="Target file pattern to match for yaml",
)
@click.option(
    "--workers",
    "-w",
    type=int,
    default=4,
    help="Number of thredds crawler workers to use",
)
@click.option(
    "--outfile", type=str, default="metadata.tar.gz", help="Sets the output file name"
)
def cli(thredds_catalogue, skips, select, workers, outfile):
    """Download Metadata from THREDDS server to tarball

    Example:

       \b
       Download files in directory that match `*yaml` and store them as a tar
        > thredds-to-tar -c "http://dapds00.nci.org.au/thredds/catalog/if87/2018-11-29/"
        -t ".*ARD-METADATA.yaml" -s '.*NBAR.*' -s '.*SUPPLEMENTARY.*'
         -s '.*NBART.*' -s '.*/QA/.*' -w 8 --outfile 2018-11-29.tar.gz

    """
    print(
        "Searching {thredds_catalogue} for matching files".format(
            thredds_catalogue=thredds_catalogue
        )
    )
    urls = thredds_find_glob(thredds_catalogue, skips, [select], workers)

    print("Found {0} metadata urls".format(str(len(urls))))

    yamls = download_yamls(urls, workers)

    # jam it all in a tar
    tar_opts = dict(
        name=outfile, mode="w" + tar_mode(gzip=True, xz=True, is_pipe=False)
    )
    with tarfile.open(**tar_opts) as tar:
        for yaml in yamls:
            add_txt_file(tar=tar, content=yaml[0], fname=yaml[1])

    print("Done!")


if __name__ == "__main__":
    cli()
