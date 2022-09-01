import gzip
import requests
import json
import sqlite3
import asyncio
import aiohttp

# Create a SQLite table of URLs and filesizes
con = sqlite3.connect("empirebc_data.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS in_network_files(url PRIMARY KEY UNIQUE, size)")

index_url = "https://antm-pt-preprod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2022-08-01_anthem_index.json.gz"

resp = requests.get(index_url)
json_data = json.loads(gzip.decompress(resp.content))

urls = set()

for file in json_data["reporting_structure"][0]["in_network_files"]:
    urls.add(file["location"])

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

print("Fetching URLs and their sizes...")
asyncio.run(fetch_url_sizes("in_network_files", urls))
total = cur.execute("SELECT SUM(size) FROM in_network_files").fetchone()[0]

print(f"Total filesize in GB: {total//1_000_000_000}")