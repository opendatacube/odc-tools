"""Thredds crawling and YAML fetching utilities
"""
from multiprocessing.dummy import Pool as ThreadPool
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
from thredds_crawler.crawl import Crawl

from ..cloud._version import __version__


def thredds_find_glob(
    base_catalog: str, skips: List[str], select: List[str], workers: int = 8
) -> List[str]:
    """Glob YAML's from base Thredds Catalog recursively
    Arguments:
        base_catalog {str} -- Base of the catlog to crawl from
        user_skips {list} -- Paths to skip in addition to NCI specific defaults
        select {list} -- Paths to select (useful YAML's)
        workers {int} -- Number of workers to use for Thredds Crawling
    Returns:
        list -- List of Thredds hosted dataset YAML url's to Index
    """
    user_skips = Crawl.SKIPS
    user_skips = user_skips.extend(skips)

    results = Crawl(
        base_catalog + "/catalog.xml", select=select, skip=user_skips, workers=workers
    ).datasets

    urls = [
        service["url"]
        for dataset in results
        for service in dataset.services
        if service["service"].lower() == "httpserver"
    ]

    return urls


def download_yamls(
    yaml_urls: List[str], workers: int = 8
) -> List[Tuple[Optional[bytes], str, Optional[str]]]:
    """Download all YAML's in a list of URL's and generate content
    Arguments:
        yaml_urls {list} -- List of URL's to download YAML's from
        workers {int} -- Number of workers to use for Thredds Downloading
    Returns:
        list -- tuples of contents and filenames
    """
    # use a threadpool to download from thredds
    pool = ThreadPool(workers)
    yamls = pool.map(_download, yaml_urls)
    pool.close()
    pool.join()

    return yamls


def _download(url: str) -> Tuple[Optional[bytes], str, Optional[str]]:
    """Internal method to download YAML's from thredds via requests
    Arguments:
        url {str} -- URL on thredds to download YAML for
    Returns:
        tuple -- URL content, target file and placeholder for error
    """
    parsed_uri = urlparse(url)
    target_filename = url[len(parsed_uri.scheme + "://") :]
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            return (resp.content, target_filename, None)
        else:
            return (None, target_filename, "Yaml not found")
    except Exception as e:
        return (None, target_filename, "Thredds Failed")
