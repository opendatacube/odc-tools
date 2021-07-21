from concurrent.futures import ThreadPoolExecutor, as_completed
from types import SimpleNamespace
from io import BytesIO
from gzip import GzipFile
import csv
import json

from . import s3_client, s3_fetch, s3_ls_dir


def find_latest_manifest(prefix, s3, **kw):
    manifest_dirs = sorted(s3_ls_dir(prefix, s3=s3, **kw), reverse=True)

    for d in manifest_dirs:
        if d.endswith("/"):
            leaf = d.split("/")[-2]
            if leaf.endswith("Z"):
                return d + "manifest.json"


def retrieve_manifest_files(key: str, s3, schema, **kw):

    bb = s3_fetch(key, s3=s3, **kw)
    gz = GzipFile(fileobj=BytesIO(bb), mode="r")
    csv_rdr = csv.reader(l.decode("utf8") for l in gz)
    for rec in csv_rdr:
        rec = SimpleNamespace(**{k: v for k, v in zip(schema, rec)})
        yield rec


def list_inventory(manifest, s3=None, prefix: str = '', suffix: str = '', contains: str = '', **kw):
    """Returns a generator of inventory records

    manifest -- s3:// url to manifest.json or a folder in which case latest one is chosen.
    """
    s3 = s3 or s3_client()

    if manifest.endswith("/"):
        manifest = find_latest_manifest(manifest, s3, **kw)

    info = s3_fetch(manifest, s3=s3, **kw)
    info = json.loads(info)

    must_have_keys = {"fileFormat", "fileSchema", "files", "destinationBucket"}
    missing_keys = must_have_keys - set(info)
    if missing_keys:
        raise ValueError("Manifest file haven't parsed correctly")

    if info["fileFormat"].upper() != "CSV":
        raise ValueError("Data is not in CSV format")

    s3_prefix = "s3://" + info["destinationBucket"].split(":")[-1] + "/"
    data_urls = [s3_prefix + f["key"] for f in info["files"]]
    schema = tuple(info["fileSchema"].split(", "))

    with ThreadPoolExecutor(max_workers=1000) as executor:
        tasks = [
            executor.submit(
                retrieve_manifest_files,
                key,
                s3,
                schema
            )
            for key in data_urls
        ]

        for future in as_completed(tasks):
            for key in future.result():
                if (
                    key.startswith(prefix) and
                    key.endswith(suffix) and
                    contains in key
                ):
                    yield key
