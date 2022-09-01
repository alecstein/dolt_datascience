import glob
import json
import sqlite3
import asyncio
import aiohttp
from tqdm import tqdm

con = sqlite3.connect("anthem_data.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS in_network_files(url PRIMARY KEY UNIQUE, size)")

files = glob.glob('./2022-09-01_anthem_index_json/*')

urls = set()

print("Loading JSONL files...")
for file in tqdm(files):
	with open(file) as f:
		for line in f:
			try: 
				data = json.loads(line)
			except json.decoder.JSONDecodeError:
				pass
			in_network_files = data['in_network_files']
			for in_network_file in in_network_files:
				urls.add(in_network_file['location'])

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

print(f"Fetching {len(urls)} URLs and their sizes...")
asyncio.run(fetch_url_sizes("in_network_files", urls))
total = cur.execute("SELECT SUM(size) FROM in_network_files").fetchone()[0]

print(f"Total filesize in GB: {total//1_000_000_000}")
