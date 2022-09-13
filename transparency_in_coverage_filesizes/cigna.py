import json
import multiprocessing as mp
import requests

import requests
import sqlite3
import aiohttp
import asyncio
from tqdm import tqdm

# Create a SQLite table of URLs and filesizes
con = sqlite3.connect("cigna_data.db")
cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS in_network_files;") 
cur.execute("CREATE TABLE IF NOT EXISTS in_network_files (id INTEGER PRIMARY KEY AUTOINCREMENT, url, size)")
cur.execute("DROP TABLE IF EXISTS allowed_amount_files;")
cur.execute("CREATE TABLE IF NOT EXISTS allowed_amount_files (id INTEGER PRIMARY KEY AUTOINCREMENT, url, size)")

def get_file_size(location):
    file_url = location
    file_size = requests.get(file_url, stream=True).headers.get('Content-length', -1)
    return (file_size, file_url)

async def fetch_url_sizes(urls, table):
    """
    Simple async function for getting all the file sizes in a list of urls
    and writing those to a SQLite table
    """
    
    session = aiohttp.ClientSession()
    fs = [session.head(url) for url in urls]
    for i, f in enumerate(asyncio.as_completed(fs)):
        resp = await f
        url = str(resp.url)
        size = int(resp.headers.get("Content-Length", -1))
        cur.execute(f"""INSERT OR IGNORE INTO {table} (url, size) VALUES ('{url}', '{size}')""")
        con.commit()
        
    await session.close()


if __name__ == '__main__':
    files_url = 'https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/table-of-contents/reporting_month=2022-09/2022-09-01_cigna-health-life-insurance-company_index.json?Expires=1665775635&Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cHM6Ly9kMjVrZ3o1cmlra3E0bi5jbG91ZGZyb250Lm5ldC9jb3N0X3RyYW5zcGFyZW5jeS9tcmYvdGFibGUtb2YtY29udGVudHMvcmVwb3J0aW5nX21vbnRoPTIwMjItMDkvMjAyMi0wOS0wMV9jaWduYS1oZWFsdGgtbGlmZS1pbnN1cmFuY2UtY29tcGFueV9pbmRleC5qc29uIiwiQ29uZGl0aW9uIjp7IkRhdGVMZXNzVGhhbiI6eyJBV1M6RXBvY2hUaW1lIjoxNjY1Nzc1NjM1fX19XX0_&Signature=JxtzQv0Jc-UuW-wTw60ttxaMvjTaS-FuI~1KiqN8l0dvI3ZwPpQ-BdNJWLCyQgQeXY7-kkcphMURsC~Rc8x5KirYGcPqREAxXHqEmdqiZDrJa3~ebwtS88IZQ4Ir8qXyWj3VMWFZQkzdNQV-QTGLPf-gfZ25XxUqqiwBAF0mJ1ZblcQc9rPuYU3lNlSjmf2ZJG8E7CpnrchleMAbTR0IQihg92Zf~QArK5WFExxJ3h8hxeehvDdBAiznwTcdjhFvMUMcYUmO3yJ1wz0MeISDOzm7Wwzp6zJ6RrAQrkgLKXKrrsSV2tqKtUxVfYiaL3aNDagOry4hOAXsIkrVQz85ew__&Key-Pair-Id=K1NVBEPVH9LWJP'
    files = requests.get(files_url).json()

    allowed_amount_files, in_network_files, sucess = [], [], None
    for i, rs in enumerate(files['reporting_structure']):
        record = rs.get('allowed_amount_file')
        if record is not None:
            allowed_amount_files.append(record['location'])
            success = True
        elif record is None:
            record = rs.get('in_network_files')
            if type(record) == list:
                record = record[0]
                in_network_files.append(record['location'])
                success = True
            elif record is not None:
                in_network_files.append(record['location'])
                success = True
        if success is None:
            print('not handled')
            print(i, rs)

    print("Fetching URLs and their sizes...")
    
    asyncio.run(fetch_url_sizes(allowed_amount_files, 'allowed_amount_files'))
    asyncio.run(fetch_url_sizes(in_network_files, 'in_network_files'))
    
    allowed_amount_files_total = cur.execute("SELECT SUM(size) FROM allowed_amount_files").fetchone()[0]
    print(f"allowed_amount_files length: {len(allowed_amount_files)}")
    print(f"allowed_amount_files unique length: {len(set(allowed_amount_files))}")
    print(f"allowed_amount_files_total filesize in GB: {allowed_amount_files_total/1000000000}")
    
    in_network_files_total = cur.execute("SELECT SUM(size) FROM in_network_files").fetchone()[0]
    print(f"in_network_files unique length: {len(set(in_network_files))}")
    print(f"in_network_files_total filesize in GB: {in_network_files_total/1000000000}")
    print("----------------------")
    print(f"allowed_amount_files_total + in_network_files_total filesize in GB: {(allowed_amount_files_total + in_network_files_total)/1000000000}")








