from atproto import Client

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
            print(item.subject.handle)
            members.append(item.uri)

    cursor = response.cursor
    print(f'cursor: {response.cursor}')
    if cursor == None:
        break

print(response.list.list_item_count)

print(len(members))


    
