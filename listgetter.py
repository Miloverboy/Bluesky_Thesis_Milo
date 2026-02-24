import re

from atproto import Client
import json

client = Client(base_url="https://public.api.bsky.app")

list_uri = "at://did:plc:kkf4naxqmweop7dv4l2iqqf5/app.bsky.graph.list/3jzmo456b6j2t"

cursor = None

members = []

while True:
    response = client.app.bsky.graph.get_list({
        "list": list_uri,
        "limit": 100,
        "cursor": cursor
    })

    if response.items:
        for item in response.items:
            #print(item.subject.handle)
            members.append(item.subject.did)
            print(item.subject.did)

    cursor = response.cursor
    if cursor == None:
        break
    
with open('news_dids.json', 'w') as f:
    json.dump(members, f)
