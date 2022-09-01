import requests
import json
import sqlite3
import asyncio
import aiohttp

con = sqlite3.connect("./kaiser_data.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS in_network_files(url PRIMARY KEY UNIQUE, size)")

def amend_url(url):
    return 'https://healthy.kaiserpermanente.org/pricing/innetwork' + url

resp = requests.get('https://healthy.kaiserpermanente.org/pricing/innetwork/2022-08_List.txt')
urls = []
for line in resp.text.split('\n'):
    urls.append(
        amend_url(line.split('  ')[0].strip().replace(' ', ''))
    )
    
urls = [url for url in urls if 'in-network-rates' in url]

async def fetch_url_sizes(table, urls):
    """
    simple async function for getting all the file sizes in a list of urls
    and writing those to a SQLite table
    """

    async with aiohttp.ClientSession() as session:
        
        fs = [session.head(url) for url in urls]
    
        for f in asyncio.as_completed(fs):
            resp = await f
            url = str(resp.url)
            size = int(resp.headers.get('content-length', -1))
            cur.execute(f"""INSERT OR IGNORE INTO {table} VALUES ("{url}", {size})""")
            con.commit()

asyncio.run(fetch_url_sizes('in_network_files', urls))

total = cur.execute("SELECT SUM(size) FROM in_network_files").fetchone()[0]

print(total/1_000_000)