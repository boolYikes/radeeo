# radio incoming input stream test
import requests
url = "https://hdradiorock-rfritschka.radioca.st/;"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:137.0) Gecko/20100101 Firefox/137.0",
    "Accept": "audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,video/*;q=0.6,*/*;q=0.5",
    "Accept-Language": "en-US,en;q=0.5",
    "Range": "bytes=64383-",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Referer": "https://hd-radio.net/",
    "Sec-Fetch-Dest": "audio",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "cross-site",
    "Accept-Encoding": "identity",
    "Priority": "u=4"
}

res = requests.get(url, headers=headers, stream=True)

with open("stream_sample.mp3", "wb") as f:
    for chunk in res.iter_content(chunk_size=1024):
        if chunk:
            f.write(chunk)