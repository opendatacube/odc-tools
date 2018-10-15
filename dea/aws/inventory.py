from types import SimpleNamespace
from io import BytesIO
from gzip import GzipFile
import csv
import json

from . import make_s3_client, s3_fetch, s3_ls_dir


def find_latest_manifest(prefix, s3):
    manifest_dirs = sorted(s3_ls_dir(prefix, s3=s3), reverse=True)

    for d in manifest_dirs:
        if d.endswith('/'):
            leaf = d.split('/')[-2]
            if leaf.endswith('Z'):
                return d + 'manifest.json'


def list_inventory(manifest, s3=None):
    """ Returns a generator of inventory records

    manifest -- s3:// url to manifest.json or a folder in which case latest one is chosen.
    """
    s3 = s3 or make_s3_client()

    info = s3_fetch(manifest, s3=s3)
    info = json.loads(info)

    must_have_keys = {'fileFormat', 'fileSchema', 'files', 'destinationBucket'}
    missing_keys = must_have_keys - set(info)
    if missing_keys:
        raise ValueError("Manifest file haven't parsed correctly")

    fileFormat = info['fileFormat']
    if fileFormat.upper() != 'CSV':
        raise ValueError('Data is not in CSV format')

    prefix = 's3://' + info['destinationBucket'].split(':')[-1] + '/'
    schema = tuple(info['fileSchema'].split(', '))
    data_urls = [prefix + f['key'] for f in info['files']]

    for u in data_urls:
        bb = s3_fetch(u, s3=s3)
        gz = GzipFile(fileobj=BytesIO(bb), mode='r')
        csv_rdr = csv.reader(l.decode('utf8') for l in gz)

        for rec in csv_rdr:
            rec = SimpleNamespace(**{k: v for k, v in zip(schema, rec)})
            yield rec
