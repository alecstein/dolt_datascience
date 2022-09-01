import requests
import sqlite3
import aiohttp
import asyncio
from tqdm import tqdm

# Create a SQLite table of URLs and filesizes
con = sqlite3.connect("aetna_data.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS in_network_files (url PRIMARY KEY UNIQUE, size)")

# The following values were inferred from looking at the network requests on the pages linked from here:
# https://www.aetna.com/individuals-families/member-rights-resources/rights/disclosure-information.html
brand_codes = ["ASH", "AETNACVS", "ALICUNDER100", "ALICFI"]
def resolve_urls(file_paths, brand_code):
    return [
        f"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/{brand_code}/2022-08-05/inNetworkRates/{file_path}" 
        for file_path in file_paths]

urls = []

# You need to loop through each brand code to get all of the in-network files
print("Looping through separate brand codes...")
for brand_code in tqdm(brand_codes):
        url = f"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/{brand_code}/latest_metadata.json"
        
        resp = requests.get(url)
        
        file_list = resp.json()["files"]
        file_paths = set([file["fileName"] for file in file_list if file["fileSchema"] == "IN_NETWORK_RATES"])
        new_urls = resolve_urls(file_paths, brand_code)
        urls.extend(new_urls)

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
