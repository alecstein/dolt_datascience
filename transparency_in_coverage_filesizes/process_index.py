"""
This module demonstrates how to download and process the in-network
files in an index.json file using python's asyncio libraries.

Example:
        (Limits are good for debugging)
        $ python process_index.py --file 2022-09-01_anthem_index.json --limit 1000

Example:
        $ python process_index.py --file 2022-09-01_anthem_index.json

Todo:
    * Show an example of how to stream through JSON files and store the 
    data in the appropriate schema
"""


import asyncio
import argparse
import aiohttp  # pip install aiohttp
import aiofile  # pip install aiofile
import ijson    # pip install ijson
import re
import gzip
import os

parser = argparse.ArgumentParser(description='Process an index.json file')
parser.add_argument("--file", help="Filename of index.json file to be processed")
parser.add_argument("--limit", help="Limit the number of in-network files to download (helpful for debugging)", default = None)
args = parser.parse_args()


tmp_dir = "./temp"
if not os.path.exists(tmp_dir):
    os.mkdir(tmp_dir)


def parse_filesize(resp):
    if (s := resp.headers.get("Content-Length")):
        return int(s)


def parse_filename(resp):
    """Turns a URL into a filename
    """
    if (d := resp.headers.get("Content-Disposition")):
        filename = re.findall("filename=(.+);", d)[0]
        return filename

    url = str(resp.url)
    filename = os.path.basename(url).split("?")[0]
    return filename


async def process_gjson_resp(resp):
    """Takes json.gz response and processes it. This
    example function saves the data to a file, then
    immediately removes the file.
    """
    filename = os.path.join(tmp_dir, parse_filename(resp))

    data = await resp.read()
    ext = gzip.decompress(data)

    async with aiofile.async_open(filename, "wb") as f:
        await f.write(ext)

    # "Processing" goes here. It's up to you what you do with
    # the saved file. Note that it's more reliable to save
    # files before processing than to stream them.
    
    os.remove(filename)
    return (filename)


async def process_gcsv_resp(resp):
    """Takes json.gz response and processes it. This
    example function saves the data to a file, then
    immediately removes the file.
    """
    filename = os.path.join(tmp_dir, parse_filename(resp))
    return (filename)


async def process_resp(resp):
    """Do something with the in-network file response.
    """

    assert resp.status == 200
    filename = parse_filename(resp)
    params = None

    if parse_filesize(resp) > 10_000_000:
        print(f"File > 10MB. Skipping: {filename}")
        return
    
    if filename.endswith(".json.gz"):
        # Compressed gJSON
        params = await process_gjson_resp(resp)

    elif filename.endswith("csv.gz"):
        # Compressed CSV
        params = await process_gcsv_resp(resp)

    print(f"Processed file: {params}")


async def fetch_and_process_in_network_urls(urls):
    """Loop through URLS and process them
    """
    async with aiohttp.ClientSession() as client:
        fs = [client.get(url) for url in urls]
        for f in asyncio.as_completed(fs):
            resp = await f
            await process_resp(resp)


def get_in_network_file_urls_from_index(index_file, limit = None):
    """Given an index file, loop through the in-network files
    and return their URLs.
    """
    with open(index_file) as f:
        objs = ijson.items(f, "reporting_structure.item.in_network_files.item.location")
        if limit:
            urls = [next(objs) for i in range(limit)]
        else:
            urls = [url for url in objs]
        # Optional filter
        urls = [url for url in urls if "in-network-rates" in url]

    return urls


async def main():
    urls = get_in_network_file_urls_from_index(args.file, limit = int(args.limit))
    await fetch_and_process_in_network_urls(urls)
    os.rmdir(tmp_dir)

asyncio.run(main())


