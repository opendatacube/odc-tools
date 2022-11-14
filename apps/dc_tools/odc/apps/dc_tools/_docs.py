"""These should probably be in datacube library."""
import json
import sys
from typing import Sequence, Union
from uuid import UUID, uuid5

from datacube.index.hl import Doc2Dataset
from datacube.utils.documents import parse_yaml

# Some random UUID to be ODC namespace
ODC_NS = UUID("6f34c6f4-13d6-43c0-8e4e-42b6c13203af")


def odc_uuid(
    algorithm: str,
    algorithm_version: str,
    sources: Sequence[Union[UUID, str]],
    deployment_id: str = "",
    **other_tags,
) -> UUID:
    """
    Generate deterministic UUID for a derived Dataset.

    :param algorithm: Name of the algorithm
    :param algorithm_version: Version string of the algorithm
    :param sources: Sequence of input Dataset UUIDs
    :param deployment_id: Some sort of identifier for installation that performs
                          the run, for example Docker image hash, or dea module version on NCI.
    :param **other_tags: Any other identifiers necessary to uniquely identify dataset
    """
    tags = [f"{k}={str(v)}" for k, v in other_tags.items()]

    stringified_sources = (
        [str(algorithm), str(algorithm_version), str(deployment_id)]
        + sorted(tags)
        + [str(u) for u in sorted(sources)]
    )

    srcs_hashes = "\n".join(s.lower() for s in stringified_sources)
    return uuid5(ODC_NS, srcs_hashes)


def from_metadata_stream(metadata_stream, index, **kwargs):
    """
    Raw metadata to Dataset stream converter.

    Given a stream of (uri, metadata) tuples convert them into Datasets, using
    supplied index and options for Doc2Dataset.


    **kwargs**:
    - skip_lineage
    - verify_lineage
    - fail_on_missing_lineage
    - products
    - exclude_products

    returns a sequence of tuples where each tuple is either

        (Dataset, None) or (None, error_message)
    """
    doc2ds = Doc2Dataset(index, **kwargs)

    for uri, metadata in metadata_stream:
        if metadata is None:
            yield (None, f"Error: empty doc {uri}")
        else:
            ds, err = doc2ds(metadata, uri)
            if ds is not None:
                yield (ds, None)
            else:
                yield (None, f"Error: {uri}, {err}")


def parse_doc_stream(doc_stream, on_error=None, transform=None):
    """
    Replace doc bytes/strings with parsed dicts.

       Stream[(uri, bytes)] -> Stream[(uri, dict)]


    :param doc_stream: sequence of (uri, doc: bytes|string)
    :param on_error: Callback uri, doc -> None
    :param transform: dict -> dict if supplied also apply further transform on parsed document

    On output doc is replaced with python dict parsed from yaml, or with None
    if parsing/transform error occurred.
    """
    for uri, doc in doc_stream:
        try:
            if uri.endswith(".json"):
                metadata = json.loads(doc)
            else:
                metadata = parse_yaml(doc)

            if transform is not None:
                metadata = transform(metadata)
        except Exception:  # pylint: disable=broad-except
            if on_error is not None:
                on_error(uri, doc)
            metadata = None

        yield uri, metadata


def from_yaml_doc_stream(doc_stream, index, logger=None, transform=None, **kwargs):
    """
    Stream of yaml documents to a stream of Dataset results.

    Stream[(path, bytes|str)] -> Stream[(Dataset, None)|(None, error_message)]

    :param doc_stream: sequence of (uri, doc: byges|string)
    :param on_error: Callback uri, doc -> None
    :param logger:  Logger object for printing errors or None
    :param transform: dict -> dict if supplied also apply further transform on parsed document
    :param kwargs: passed on to from_metadata_stream

    """

    def on_parse_error(uri, doc):
        # pylint: disable=unused-argument
        if logger is not None:
            logger.error(f"Failed to parse: {uri}")
        else:
            print(f"Failed to parse: {uri}", file=sys.stderr)

    metadata_stream = parse_doc_stream(
        doc_stream, on_error=on_parse_error, transform=transform
    )
    return from_metadata_stream(metadata_stream, index, **kwargs)
