import ijson
import os

# Change this to the file of your choice
filename = "/Users/alecstein/dolthub/bounties/transparency-in-coverage/2022-07-01_Ascent-Hospitality-Management_PS1-50_C2_in-network-rates.json"

file_size = os.stat(filename)
size_mb = file_size.st_size/1_000_000
print(f"JSON In-Network filesize: {size_mb} MB")

price_count = 0
with open(filename) as f:
    objs = ijson.items(f, "in_network.item.negotiated_rates.item.negotiated_prices")
    for o in objs:
        price_count += 1
        if price_count % 100_000 == 0:
            print(f"Running count of prices while scanning file: {price_count}", end = "\r")

print(f"There are {price_count/size_mb} prices per MB in this JSON file.")