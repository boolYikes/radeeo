import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
SEARCH_QUERY = "The HU - Yuve Yuve Yu"
MAX_RESULTS = 1

url = "https://www.googleapis.com/youtube/v3/search"
params = {
    "part": "snippet",
    "q": SEARCH_QUERY,
    "key": API_KEY,
    "type": "video",
    "maxResults": MAX_RESULTS
}

response = requests.get(url, params=params)
data = response.json()

# Get the first video ID
video_id = data["items"][0]["id"]["videoId"]
print("YouTube Video ID:", video_id)
print("Full URL: https://www.youtube.com/watch?v=" + video_id)
