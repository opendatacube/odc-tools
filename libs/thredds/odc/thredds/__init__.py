"""Thredds crawling and YAML fetching utilities
"""
from thredds_crawler.crawl import Crawl
import requests
from urllib.parse import urlparse


def thredds_find_glob(
    base_catalog: str, skips: list, select: list, workers: int = 8
) -> list:
    """Glob YAML's from base Thredds Catalog recursively
    
    Arguments:
        base_catalog {str} -- Base of the catlog to crawl from
        user_skips {list} -- Paths to skip in addition to NCI specific defaults
        select {list} -- Paths to select (useful YAML's)
        workers {int} -- Number of workers to use for Thredds Crawling
    
    Returns:
        list -- List of datasets to Index
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


def download_yamls(yaml_urls: list) -> list:
    """Download all YAML's in a list of URL's and generate 
    
    Arguments:
        yaml_urls {list} -- List of URL's to download YAML's from
    
    Returns:
        list -- tuples of contents and filenames
    """
    # TODO: Make this parallel with Asyncio or Multi-processing
    yaml_collection = []
    for url in yaml_urls:
        try:
            yaml_collection.append(_download(url))
        except Exception as e:
            # Stash errors
            yaml_collection.append((None, None, e))

    return yaml_collection


def _download(url: str) -> tuple:
    """Internal method to download YAML's from thredds via requests
    
    Arguments:
        url {str} -- URL on thredds to download YAML for
    
    Raises:
        Exception: Exception to indicate YAML is not valid location
    
    Returns:
        tuple -- URL content, target file and placeholder for error
    """
    parsed_uri = urlparse(url)
    target_filename = url[len(parsed_uri.scheme + "://") :]
    resp = requests.get(url)
    if resp.status_code == 200:
        return (resp.content, target_filename, None)
    else:
        raise Exception("Yaml not found")
