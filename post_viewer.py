import requests

uri = 'at://did:plc:7ia5gyyqv2will5wxhrdey5h/app.bsky.feed.post/3m7vip733rq2o'
url = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"

# response = requests.get(url, params={"uris": uris})

# if response.status_code == 200:
#                 data = response.json()




did = uri.split("/")[2]
rkey = uri.split("/")[-1]

url = f"https://bsky.app/profile/{did}/post/{rkey}"
print(url)
