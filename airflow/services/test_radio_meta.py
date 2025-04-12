# attention attention - shinedown
# can you roll her - ufo
# 'The HU - Yuve Yuve Yu',
# 'Triumph - Never Surrender'
# "Rick Springfield - Jessie's Girl"
# Run-D.M.C. - Walk This Way
# Run-D.M.C. - It's Tricky
# Metallica - Mama Said
# Buckcherry - Crazy Bitch
import requests
url = "https://rosetta.shoutca.st/external/rpc.php?m=streaminfo.get&username=hdradiorock&rid=hdradiorock"

params = {
    "m": "streaminfo.get",
    "username": "hdradiorock",
    "rid": "hdradiorock"
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:137.0) Gecko/20100101 Firefox/137.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Referer": "https://hd-radio.net/",
    "Sec-Fetch-Dest": "script",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "cross-site",
    "Pragma": "no-cache"
}

res = requests.get(url, headers=headers, params=params)

data = res.json()

db.append(data["data"][0])