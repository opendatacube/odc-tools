import botocore
from . import (
    _aws_unsigned_check_env,
    _mk_s3_client,
    s3_fmt_range,
    s3_url_parse,
    ByteRange,
)
from ._find import norm_predicate, s3_file_info
from typing import IO, Any, Dict, Optional, Union
from botocore.credentials import ReadOnlyCredentials
from botocore.exceptions import ClientError
from botocore.session import Session


class S3Client:
    def __init__(
        self,
        profile: Optional[str] = None,
        creds: Optional[ReadOnlyCredentials] = None,
        region_name: Optional[str] = None,
        session: Optional[Session] = None,
        aws_unsigned: Optional[bool] = None,
        use_ssl: bool = True,
        **cfg,
    ):
        if aws_unsigned is None:
            aws_unsigned = _aws_unsigned_check_env()

        if aws_unsigned:
            cfg.update(signature_version=botocore.UNSIGNED)

        self.s3_client = _mk_s3_client(
            profile,
            creds=creds,
            region_name=region_name,
            session=session,
            use_ssl=use_ssl,
            **cfg,
        )

    def open(
        self,
        url: str,
        range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
        **kwargs,
    ):
        """Open whole or part of S3 object

        :param url: s3://bucket/path/to/object
        :param s3: pre-configured s3 client, see make_s3_client()
        :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
        :param kwargs: are passed on to ``s3.get_object(..)``
        """
        if range is not None:
            try:
                kwargs["Range"] = s3_fmt_range(range)
            except Exception:
                raise ValueError("Bad range passed in: " + str(range)) from None

        bucket, key = s3_url_parse(url)
        oo = self.s3_client.get_object(Bucket=bucket, Key=key, **kwargs)
        return oo["Body"]

    def download(
        self,
        url: str,
        destination: Optional[str] = None,
        range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
        read_chunk_size: int = 10 * (1 << 20),  # 10Mb
        **kwargs,
    ) -> str:
        """
        Download file from S3 to local storage

        :param url: Source object
        :param destination: Output file name (defaults to object name in current directory)
        :param s3: pre-configured s3 client, see make_s3_client()
        :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
        :param read_chunk_size: How many bytes to read at a time (default 10Mb)
        :param kwargs: are passed on to ``s3.get_object(..)``

        :returns: destination file path as a string
        """
        if destination is None:
            bucket, key = s3_url_parse(url)
            destination = key.split("/")[-1]

        src = self.open(url, range=range, **kwargs)
        with open(destination, "wb") as dst:
            for chunk in src.iter_chunks(read_chunk_size):
                dst.write(chunk)

        return destination

    def s3_head_object(self, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Head object, return object metadata.

        :param url: s3://bucket/path/to/object
        :param s3: pre-configured s3 client, see make_s3_client()
        :param kwargs: are passed on to ``s3.head_object(..)``
        """
        bucket, key = s3_url_parse(url)

        try:
            oo = self.s3_client.head_object(Bucket=bucket, Key=key, **kwargs)
        except ClientError:
            return None

        meta = oo.pop("ResponseMetadata", {})
        code = meta.get("HTTPStatusCode", 0)
        if 200 <= code < 300:
            return oo

        # it actually raises exceptions when http code is in the "fail" range
        return None  # pragma: no cover

    def fetch(
        self,
        url: str,
        range: Optional[ByteRange] = None,  # pylint: disable=redefined-builtin
        **kwargs,
    ) -> bytes:
        """Read entire or part of object into memory and return as bytes

        :param url: s3://bucket/path/to/object
        :param s3: pre-configured s3 client, see make_s3_client()
        :param range: Byte range to read (first_byte, one_past_last_byte), default is whole object
        """
        return self.open(url, range=range, **kwargs).read()

    def dump(self, data: Union[bytes, str, IO], url: str, **kwargs):
        """Write data to s3 object.

        :param data: bytes to write
        :param url: s3://bucket/path/to/object
        :param s3: pre-configured s3 client, see s3_client()
        :param kwargs: Are passed on to ``s3.put_object(..)``

        ContentType
        ACL
        """

        bucket, key = s3_url_parse(url)

        r = self.s3_client.put_object(Bucket=bucket, Key=key, Body=data, **kwargs)
        code = r["ResponseMetadata"]["HTTPStatusCode"]
        return 200 <= code < 300

    def ls_all(self, url, **kw):
        bucket, prefix = s3_url_parse(url)

        paginator = self.s3_client.get_paginator("list_objects_v2")

        n_skip = len(prefix)
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, **kw):
            for o in page.get("Contents", []):
                yield o["Key"][n_skip:]

    def ls_dir(self, uri, **kw):
        bucket, prefix = s3_url_parse(uri)

        if len(prefix) > 0 and not prefix.endswith("/"):
            prefix = prefix + "/"

        paginator = self.s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=bucket, Prefix=prefix, Delimiter="/", **kw
        ):
            sub_dirs = page.get("CommonPrefixes", [])
            files = page.get("Contents", [])

            for p in sub_dirs:
                yield f"s3://{bucket}/{p['Prefix']}"

            for o in files:
                yield f"s3://{bucket}/{o['Key']}"

    def find(self, url, pred=None, glob=None, **kw):
        """List all objects under certain path

        each s3 object is represented by a SimpleNamespace with attributes:
        - url
        - size
        - last_modified
        - etag
        """
        if glob is None and isinstance(pred, str):
            pred, glob = None, pred

        pred = norm_predicate(pred, glob)

        if url[-1] != "/":
            url += "/"

        bucket, prefix = s3_url_parse(url)

        paginator = self.s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, **kw):
            for o in page.get("Contents", []):
                o = s3_file_info(o, bucket)
                if pred is not None and pred(o):
                    yield o
