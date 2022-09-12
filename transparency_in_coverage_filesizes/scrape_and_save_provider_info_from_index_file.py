import gzip
import ijson
import requests
import sqlite3
import aiohttp
import asyncio
import os
import argparse
from glob import glob

"""
Usage:
> python3 tableize_provider_info.py --filename tmp/2022-08-01_anthem_index.json --cleanup 1 --limit 10
setting cleanup to 1 or 0 will delete or keep the downloaded in-network files

TODOs: 
1. make async download of in-network files so that you can begin processing before you download all of them
2. make price data table
"""

parser = argparse.ArgumentParser(description='Get provider groups from index file.')
parser.add_argument("--filename", help="index filename to be processed")
parser.add_argument("--cleanup", help="Delete downloaded in-network-rates files as you go", default=1)
parser.add_argument("--limit", help="limit the number of files to download", default=0)
args = parser.parse_args()

index_file = args.filename

tmp_dir = "./tmp"

if not os.path.exists(tmp_dir):
    os.mkdir(tmp_dir)

index_file_basename = os.path.basename(os.path.splitext(index_file)[0])

con = sqlite3.connect(f"{index_file_basename}_data.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS provider_data(index_file_basename, npi, tin_type, tin_value, provider_group_id, PRIMARY KEY (index_file_basename, npi, tin_type, tin_value, provider_group_id))")
# cur.execute("CREATE TABLE IF NOT EXISTS price_data()

def get_in_network_files_list(index_file):
    with open(index_file) as f:
        plans = ijson.items(f, "reporting_structure.item")
        for plan in plans:
            in_network_files_metadata_list = plan['in_network_files']
            reporting_plans = plan['reporting_plans']            

        print(f"Getting metadata to download in-network files...")
        if args.limit != 0:
            print(f"Limiting to first {args.limit} files.")
        
        return in_network_files_metadata_list


def fetch_and_save_in_network_files(in_network_files_metadata_list, tmp_dir):
    i = 0
    for in_network_file_metadata in in_network_files_metadata_list:
        i += 1
        in_network_file_url =  in_network_file_metadata['location']
        in_network_file_data = requests.get(in_network_file_url, stream = True)
        
        # Strip off anything after ? from the filename
        in_network_file_name = os.path.join(
            tmp_dir, 
            os.path.basename(in_network_file_url).split('?')[0]
        )

        with open(in_network_file_name, "wb") as f:
            f.write(in_network_file_data.content)
            print(f"Saved {in_network_file_name}")
        # remove in final version
        if (i > int(args.limit)) and (args.limit != 0):
            break


async def fetch_provider_data(session, provider_data):
    url = provider_data['location']
    async with session.get(url) as resp:
        try:
            return {'json_response': await resp.json(), 'provider_data': provider_data}
        except aiohttp.client_exceptions.ContentTypeError:
            print(f"Error processing url: {url}")
            return 
    

async def process_providers(in_network_file):
    """
    Return a flat table of all of the provider objects
    """
    
    flattened_provider_refs = {
        'npi': [],
        'tin_type': [],
        'tin_value': [],
        'provider_group_id': [],}
    
    # Track any time the provider refs link somewhere externally
    
    external_provider_refs = []
    
    try:
        gzip.open(in_network_file)
        # gzip.close(in_network_file)
    except gzip.BadGzipFile:
        print(f"Error: {in_network_file} is not properly gzipped!")
        return

    with gzip.open(in_network_file) as f:
        provider_refs = ijson.items(f, 'provider_references.item')
        for provider_ref in provider_refs:
            provider_data = {}
            
            provider_data['provider_group_id'] = provider_ref['provider_group_id']
            
            # Case that the provider location is separate
            try:
                provider_data['location'] = provider_ref['location']
                external_provider_refs.append(provider_data)
            except KeyError:
                continue
            
            # Case that provider group data is given in-file
            try:
                for group in provider_data['provider_groups']:
                    for npi in group['npi']:
                            tin_type = group['tin']['type']
                            tin_value = group['tin']['value']
                            provider_group_id = external_provider_ref['provider_data']['provider_group_id']
                                      
                            params = (index_file_basename, npi, tin_type, tin_value, str(provider_group_id))
                            transaction = f"""INSERT OR IGNORE INTO provider_data VALUES (?, ?, ?, ?, ?)"""
                            cur.execute(transaction, params)
            except KeyError:
                continue
                        
        if external_provider_refs:
            async with aiohttp.ClientSession() as session:
                tasks = [fetch_provider_data(session, provider_data) for provider_data in external_provider_refs]
                external_provider_refs = await asyncio.gather(*tasks)
                
                for external_provider_ref in external_provider_refs:
                    if not external_provider_ref:
                        continue
                    for group in external_provider_ref['json_response']['provider_groups']:
                        for npi in group['npi']:
                            
                            tin_type = group['tin']['type']
                            tin_value = group['tin']['value']
                            provider_group_id = external_provider_ref['provider_data']['provider_group_id']
                                      
                            params = (index_file_basename, npi, tin_type, tin_value, str(provider_group_id))
                            transaction = f"""INSERT OR IGNORE INTO provider_data VALUES (?, ?, ?, ?, ?)"""
                            cur.execute(transaction, params)
                            con.commit()


async def process_in_network_files(in_network_files_list, delete_files):
    
    for in_network_file in in_network_files:
        await process_providers(in_network_file)
        print(f"Processed providers for file: {in_network_file}")
        if delete_files:
            os.remove(in_network_file)
            print(f"Removed file: {in_network_file}")

in_network_files_metadata_list = get_in_network_files_list(index_file)

fetch_and_save_in_network_files(in_network_files_metadata_list, tmp_dir)

in_network_files = glob(os.path.join(tmp_dir, "*in-network-rates*.json.gz"))

asyncio.run(process_in_network_files(in_network_files, args.cleanup))