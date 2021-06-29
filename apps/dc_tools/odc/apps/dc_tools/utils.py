import logging

import pkg_resources
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

ESRI_LANDCOVER_BASE_URI = (
    "https://ai4edataeuwest.blob.core.windows.net/io-lulc/"
    "io-lulc-model-001-v01-composite-v03-supercell-v02-clip-v01/{id}_20200101-20210101.tif"
)


class IndexingException(Exception):
    """
    Exception to raise for error during SQS to DC indexing/archiving
    """

    pass


def get_esri_list():
    stream = pkg_resources.resource_stream(__name__, "./esri-lc-tiles-list.txt")
    with stream as f:
        for tile in f.readlines():
            id = tile.decode().rstrip('\n')
            yield ESRI_LANDCOVER_BASE_URI.format(id=id)


def index_update_dataset(
    metadata: dict,
    uri: str,
    dc: Datacube,
    doc2ds: Doc2Dataset,
    update=False,
    update_if_exists=False,
    allow_unsafe=False,
):
    if uri is not None:
        # Make sure we can create a dataset first
        try:
            ds, err = doc2ds(metadata, uri)
        except ValueError as e:
            raise IndexingException(
                f"Exception thrown when trying to create dataset: '{e}'\n The URI was {uri}"
            )

        # Now do something with the dataset
        if ds is not None:
            if dc.index.datasets.has(metadata.get("id")):
                # Update
                if update or update_if_exists:
                    # Set up update fields
                    updates = {}
                    if allow_unsafe:
                        updates = {tuple(): changes.allow_any}
                    # Do the updating
                    try:
                        dc.index.datasets.update(ds, updates_allowed=updates)
                    except ValueError as e:
                        raise IndexingException(
                            f"Updating the dataset raised an exception: {e}"
                        )
                else:
                    logging.warning("Dataset already exists, not indexing")
            else:
                if update:
                    # We're expecting to update a dataset, but it doesn't exist
                    raise IndexingException(
                        "Can't update dataset because it doesn't exist."
                    )
                # Everything is working as expected, add the dataset
                dc.index.datasets.add(ds)
        else:
            raise IndexingException(
                f"Failed to create dataset with error {err}\n The URI was {uri}"
            )
    else:
        raise IndexingException("Failed to get URI from metadata doc")
