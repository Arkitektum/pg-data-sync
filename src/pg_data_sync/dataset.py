import json
import re
import zipfile
import time
import os
from collections import OrderedDict
from pathlib import Path
from uuid import UUID
from datetime import datetime, date
from typing import Dict, List, Any
import aiohttp
import aiofiles
import xmltodict
from aiohttp import BasicAuth
from .models import DatasetConfig
from .utils import get_env, delete_file_or_dir, get_file_size

DOWNLOAD_API_BASE_URL = 'https://nedlasting.geonorge.no/api'
METADATA_API_URL = 'https://kartkatalog.geonorge.no/api/getdata'
DOWNLOAD_TIMEOUT = 1800


async def place_order(config: DatasetConfig) -> str:
    response = await fetch_order(config)
    files: List[Dict] = response.get('files', [])

    if not files:
        raise Exception(
            'Error placing download order: The download order did not contain any files')

    ref_number = response.get('referenceNumber')
    file_id = files[0].get('fileId')

    return f'{DOWNLOAD_API_BASE_URL}/v3/download/order/{ref_number}/{file_id}'


async def get_remote_file_size(url):
    async with aiohttp.ClientSession() as session:
        async with session.head(url) as response:
            if response.status == 200:
                content_length = response.headers.get('Content-Length')
                print(content_length)


async def download_file(url: str, filename: str):
    auth = BasicAuth(get_env('API_USERNAME'), get_env('API_PASSWORD'))
    start = time.time()

    file_path = Path(filename)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, auth=auth) as response:
                response.raise_for_status()

                content_length = response.headers.get('Content-Length')
                file_size = get_file_size(content_length)

                print(f'Downloading file ({file_size:.2f} MB)...')

                async with aiofiles.open(filename, mode='wb') as file:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        await file.write(chunk)

        print(
            f'File downloaded from "{url}" in {round(time.time() - start, 2)} sec.')
    except Exception as err:
        raise Exception(f'Error downloading file: {err}')


def extract_archive(file_path: str, out_dir: str) -> str:
    start = time.time()

    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(out_dir)

            print(
                f'Archive "{file_path}" extracted in {round(time.time() - start, 2)} sec.')
        return out_dir
    except Exception as err:
        raise Exception(f'Error extracting archive: {err}')
    finally:
        delete_file_or_dir(file_path)


def get_resource_path(out_dir: str, glob: str | None) -> str:
    if not glob:
        paths = list(Path(out_dir).glob('*'))
    else:
        paths = list(Path(out_dir).rglob(glob, case_sensitive=False))

    if not len(paths) == 1:
        raise Exception(f'Could not find resource in "{out_dir}"')

    return str(paths[0])


async def get_dataset_update_date(metadata_id: UUID, area_code: str, area_type: str, epsg: str, format: str) -> date | None:
    feed_url = await _get_feed_url(metadata_id, format)

    if not feed_url:
        return None

    xml_str = await _fetch_feed(feed_url)
    doc = xmltodict.parse(xml_str)

    update_date = _get_dataset_update_date(doc, area_code, area_type, epsg)

    return update_date


async def fetch_order(config: DatasetConfig) -> Dict[str, Any]:
    url = f'{DOWNLOAD_API_BASE_URL}/order'

    request_body = config.create_order_request_body()
    data = json.dumps(request_body)

    headers = {
        'Content-Type': 'application/json'
    }

    auth = BasicAuth(get_env('API_USERNAME'), get_env('API_PASSWORD'))

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, auth=auth, data=data) as response:
                response.raise_for_status()
                print('Download order placed')

                return await response.json()
    except Exception as err:
        raise Exception(f'Error placing download order: {err}')


def _get_dataset_update_date(doc: OrderedDict[str, Any], area_code: str, area_type: str, epsg: str) -> date | None:
    nationwide_regex = re.compile(r'^.*?landsdekkende$', re.IGNORECASE)
    code_regex = re.compile(r'^.*?(?P<code>\d+).*$')

    feed: Dict = doc.get('feed', {})
    entries: List[Dict[str, Any]] = feed.get('entry', [])
    date_str: str = ''

    for entry in entries:
        categories: List[Dict[str, str]] = []
        category_nodes = entry.get('category', [])

        if isinstance(category_nodes, dict):
            categories.append(category_nodes)
        else:
            categories = category_nodes

        crs = next((cat.get('@term') for cat in categories if cat.get('@scheme')
                    == 'http://www.opengis.net/def/crs/'), None)

        if not crs:
            continue

        parts = crs.split(':')

        if parts[-1] != epsg:
            continue

        title: str = entry.get('title', '')

        if area_type == 'landsdekkende':
            match = nationwide_regex.search(title)

            if match:
                date_str = entry.get('updated', '')
                break
        else:
            hit = any(cat.get('@term', '').lower()
                      == area_type for cat in categories)

            if not hit:
                continue

            match = code_regex.search(title)

            if not match:
                continue

            code = match.group('code')

            if code != area_code:
                continue

            date_str = entry.get('updated', '')
            break

    if not date_str:
        return None

    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        return date_obj.date()
    except:
        return None


async def _get_feed_url(metadata_id: UUID, format_name: str) -> str | None:
    response = await _fetch_dataset_metadata(metadata_id)

    dist_formats: List[Dict] = response.get('DistributionsFormats', [])

    feed_url = next((df.get('URL') for df in dist_formats if df.get(
        'Protocol') == 'W3C:AtomFeed' and df.get('FormatName') == format_name), '')

    return feed_url


async def _fetch_feed(url: str) -> str:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()

                return await response.text()
    except Exception as err:
        raise Exception(f'Error fetching feed: {err}')


async def _fetch_dataset_metadata(metadata_id: UUID) -> Dict[str, Any]:
    url = f'{METADATA_API_URL}/{metadata_id}'

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()

                return await response.json()
    except Exception as err:
        raise Exception(f'Error fetching dataset metadata: {err}')


__all__ = ['place_order', 'download_file', 'extract_archive',
           'get_resource_path', 'get_dataset_update_date', 'fetch_order']
