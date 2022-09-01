import asyncio
import aiohttp
import requests
import sqlite3
import ijson

con = sqlite3.connect("bcbs_data.db")
cur = con.cursor()


async def fetch_url_sizes(table):

    filename = '/Users/alecstein/dolthub/bounties/transparency-in-coverage/bcbs/2022-07-27_blue-cross-and-blue-shield-of-north-carolina_index.json'

    urls = []
    with open(filename) as f:
        url_objs = ijson.items(f, 'reporting_structure.item.in_network_files.item.location', multiple_values=True)
        for url in url_objs:
            try:
                urls.append(url)
                print(1)
            except ijson.common.IncompleteJSONError as e:
                print(e)
                break

    async with aiohttp.ClientSession() as session:
        fs = [session.head(url) for url in urls]

        for f in asyncio.as_completed(urls):
            resp = await f
            url = str(resp.url)
            size = int(resp.headers.get('content-length', -1))
            print(url, size)
            cur.execute(f"""INSERT OR IGNORE INTO {table} VALUES ("{url}", {size})""")
            con.commit()

asyncio.run(fetch_url_sizes('test'))