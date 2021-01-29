"""Azure blob storage crawling and YAML fetching utilities
"""
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from typing import List, Optional, Tuple

import azure
from azure.storage.blob import BlobClient, ContainerClient


def find_blobs(
    account_url: str, container_name: str, credential: str, prefix: str, suffix: str
) -> List:
    blob_list = []
    try:
        container = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=credential,
        )
        for blob_record in container.list_blobs(name_starts_with=prefix):
            blob_name = blob_record["name"]

            if blob_name.endswith(suffix):
                blob_list.append(blob_name)
    except azure.core.exceptions.ServiceRequestError as e:
        print(f"Failed to connect to Azure with error {e}")
    return blob_list


def download_yamls(
    account_url: str,
    container_name: str,
    credential: str,
    yaml_urls: List[str],
    workers: int = 32,
) -> List[Tuple[Optional[bytes], str, Optional[str]]]:
    """Download all YAML's in a list of blob names and generate content
    Arguments:
        account_url {str} -- Azure account url
        container_name {str} -- Azure container name
        credential {str} -- Azure credential token

        yaml_urls {list} -- List of URL's to download YAML's from
        workers {int} -- Number of workers to use for Thredds Downloading
    Returns:
        list -- tuples of contents and filenames
    """

    pool = ThreadPool(workers)
    yamls = pool.map(
        partial(_download, account_url, container_name, credential), yaml_urls
    )
    pool.close()
    pool.join()
    return yamls


def _download(
    account_url: str, container_name: str, credential: str, blob_name: str
) -> Tuple[Optional[bytes], str, Optional[str]]:
    """Internal method to download YAML's from Azure via BlobClient
    Arguments:
        account_url {str} -- Azure account url
        container_name {str} -- Azure container name
        credential {str} -- Azure credential token
        blob_name {str} -- Blob name to download
    Returns:
        tuple -- URL content, target file and placeholder for error
    """

    result = None
    try:
        blob = BlobClient(
            account_url=account_url,
            container_name=container_name,
            credential=credential,
            blob_name=blob_name,
        )
        result = blob.download_blob().readall()
    except azure.core.exceptions.ServiceRequestError as e:
        print(f"Failed to download {blob_name} with error {e}")

    return [result, blob_name]
