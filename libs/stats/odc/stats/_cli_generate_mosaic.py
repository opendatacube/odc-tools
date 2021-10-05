import click
from datacube import Datacube
from odc.algo import save_cog

from ._cli_common import main
from ._stac_fetch import s3_fetch_dss


@main.command("generate-mosaic")
@click.argument("input_glob", type=str)
@click.argument("output", type=str)
@click.option("--bands", type=str)
@click.option("--resampling", type=str, default="average")
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
def generate_mosaic(input_glob, output, bands, resampling, verbose):
    """
    Generate mosaic overviews of the stats data.

    Note: The input bucket must be public otherwise the data can not be listed.
    """
    bands = bands.split(",")

    if verbose:
        print(f"Preparing mosaics from: '{input_glob}'")

    dss = s3_fetch_dss(input_glob)

    dc = Datacube()
    xx = dc.load(
        datasets=dss,
        dask_chunks={"x": 2048, "y": 2048},
        resolution=(-120, 120),
        measurements=bands,
    )

    if verbose:
        print(f"Writing {output}")

    xx = xx.squeeze("time").to_stacked_array("bands", ["x", "y"])
    yy = save_cog(
        xx,
        output,
        blocksize=1024,
        compress="zstd",
        zstd_level=4,
        overview_resampling=resampling,
        NUM_THREADS="ALL_CPUS",
        bigtiff="YES",
        SPARSE_OK=True,
    )

    yy.compute()


if __name__ == "__main__":
    main()
