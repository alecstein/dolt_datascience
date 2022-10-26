import requests
import sqlite3

con = sqlite3.connect("humana_data.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS in_network_files(url PRIMARY KEY UNIQUE, size)")

params = {
    "fileType": "innetwork",
    "iDisplayLength": "2000",
    "iDisplayStart": 0
}

print("Fetching URLs and their sizes...")

finished = False
while not finished:
    resp = requests.get("https://developers.humana.com/Resource/GetData", params = params) \
                   .json()
    if len(resp['aaData']) > 0:
        for file in resp["aaData"]:
            url = "https://developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&" + file["name"]
            size = int(file["size"])
            cur.execute(f"""INSERT OR IGNORE INTO in_network_files VALUES ("{url}", {size})""")
    else:
        finished = True

    con.commit()
    
    # Update the counter to get the next batch
    params['iDisplayStart'] += len(resp['aaData'])
    print(f"\r{params['iDisplayStart']: 6d}/{resp['iTotalRecords']} ({100*params['iDisplayStart']/resp['iTotalRecords']:3.1f}%)", end='')

total = cur.execute("SELECT SUM(size) FROM in_network_files").fetchone()[0]
print(f"\nTotal filesize in GB: {total//1_000_000_000}")