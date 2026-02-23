import requests

uri = 'at://did:plc:puysacjz5dvhzf73uuv2pbxm/app.bsky.feed.post/3ma4lly3n6s24'
url = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"

# response = requests.get(url, params={"uris": uris})

# if response.status_code == 200:
#                 data = response.json()




did = uri.split("/")[2]
rkey = uri.split("/")[-1]

url = f"https://bsky.app/profile/{did}/post/{rkey}"
print(url)
