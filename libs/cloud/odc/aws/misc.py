import logging
from urllib.request import Request

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import get_session

from . import auto_find_region, s3_url_parse

log = logging.getLogger(__name__)


def s3_get_object_request_maker(region_name=None, credentials=None, ssl=True):
    session = get_session()

    if region_name is None:
        region_name = auto_find_region(session)

    if credentials is None:
        managed_credentials = session.get_credentials()
        credentials = managed_credentials.get_frozen_credentials()
    else:
        managed_credentials = None

    protocol = "https" if ssl else "http"
    auth = S3SigV4Auth(credentials, "s3", region_name)

    def maybe_refresh_credentials():
        nonlocal credentials
        nonlocal auth

        if not managed_credentials:
            return

        creds = managed_credentials.get_frozen_credentials()
        if creds is credentials:
            return

        log.debug("Refreshed credentials (s3_get_object_request_maker)")

        credentials = creds
        auth = S3SigV4Auth(credentials, "s3", region_name)

    def build_request(
        bucket=None, key=None, url=None, range=None
    ):  # pylint: disable=redefined-builtin
        if key is None and url is None:
            if bucket is None:
                raise ValueError("Have to supply bucket,key or url")
            # assume bucket is url
            url = bucket

        if url is not None:
            bucket, key = s3_url_parse(url)

        if isinstance(range, (tuple, list)):
            range = "bytes={}-{}".format(range[0], range[1] - 1)

        maybe_refresh_credentials()

        headers = {}
        if range is not None:
            headers["Range"] = range

        req = AWSRequest(
            method="GET",
            url="{}://s3.{}.amazonaws.com/{}/{}".format(
                protocol, region_name, bucket, key
            ),
            headers=headers,
        )

        auth.add_auth(req)

        return Request(req.url, headers=dict(**req.headers), method="GET")

    return build_request
