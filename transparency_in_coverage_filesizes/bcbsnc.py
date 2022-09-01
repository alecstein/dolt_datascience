import asyncio
import aiohttp
import requests
import sqlite3
import time
from bs4 import BeautifulSoup
from tqdm import tqdm

# Create a SQLite table of URLs and filesizes
con = sqlite3.connect("bcbsnc_data.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS index_files(url PRIMARY KEY UNIQUE, size)")
cur.execute("CREATE TABLE IF NOT EXISTS in_network_files(url PRIMARY KEY UNIQUE, size)")
cur.execute("CREATE TABLE IF NOT EXISTS fetched_index_files(url PRIMARY KEY UNIQUE)")

mrfs_url = "https://www.bluecrossnc.com/about-us/policies-and-best-practices/transparency-coverage-mrf"

r = requests.get(mrfs_url)
soup = BeautifulSoup(r.content, features = "lxml")

links = soup.find_all("a")
urls = []
for link in links:
    if (url := link.get("href")) is not None and "index.json" in url:
        urls.append(url)


async def fetch_url_sizes(table, urls):
    """
    Simple async function for getting all the file sizes in a list of urls
    and writing those to a SQLite table
    """
    
    session = aiohttp.ClientSession()
    fs = [session.head(url) for url in urls]

    for f in asyncio.as_completed(fs):
        resp = await f
        url = str(resp.url)
        size = int(resp.headers.get("content-length", -1))
        cur.execute(f"""INSERT OR IGNORE INTO {table} VALUES ("{url}", {size})""")
        con.commit()
        
    await session.close()
            
# Get all the MRF files on the main BCBS page
asyncio.run(fetch_url_sizes("index_files", urls))

# Sort from smallest to largest (some are multiple GB)
index_file_urls = cur.execute("SELECT url FROM index_files ORDER BY size").fetchall()

for url in tqdm(index_file_urls):

    url = url[0]

    if cur.execute(f"""SELECT url FROM fetched_index_files where url = "{url}" """).fetchone() is None:

        resp = requests.get(url, stream = True)
        size_mb = int(resp.headers["Content-Length"])/1_000_000

        if size_mb > 1_000:
            print(f"\nCannot download file of size: {size_mb} MB. Do this manually.")
            print(url)
            continue

        urls = [file["location"] for file in r.json()["reporting_structure"][0]["in_network_files"]]

        asyncio.run(fetch_url_sizes("in_network_files", urls))

        cur.execute(f"""INSERT OR IGNORE INTO fetched_index_files VALUES ("{url}")""")
        con.commit()

print("Fetching URLs and their sizes...")
asyncio.run(fetch_url_sizes("in_network_files", urls))
total = cur.execute("SELECT SUM(size) FROM in_network_files").fetchone()[0]

print(f"Total filesize in GB: {total//1_000_000_000}")