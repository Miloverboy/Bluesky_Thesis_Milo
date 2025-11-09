import asyncio
import websockets
import json
import re

uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

def is_suitable_post(message):
    if "commit" not in message or "record" not in message["commit"] or message["commit"]["record"]["$type"] != "app.bsky.feed.post":
      print('not a post')
      return False
    else:
       record = message["commit"]["record"]


    if "langs" not in record:
      print('not eng')
      return False
    elif "reply" in record:
      print('reply')
      return False
    #elif "facets" in message["commit"]["record"] and List(filter(lambda f: f['$type'] == message["commit"]["record"]["facets"]:
    #  print(json.dumps(message["commit"]["record"]["facets"], indent = 2))
    else:
      langs = record["langs"]
      if len(langs) == 1 and langs[0] == 'en': 
          try:
              record["text"].encode(encoding='utf-8').decode('ascii')
          except UnicodeDecodeError:
              #print("not english: " + message["commit"]["record"]["text"])
              return False
          else: 
              #print("english: " + message["commit"]["record"]["text"])
              return True
               
def show_fields(message):
   fields = {}
   print(type(message))
   if isinstance(message, dict):
      print('t')
       


async def listen_to_websocket():
  async with websockets.connect(uri) as websocket:
    for i in range(101):
      try:
        response = await websocket.recv()
        message = json.loads(response)
        #if message['$type'] == 'app.bsky.feed.post':
        print(json.dumps(message, indent=2))
        #print(message["commit"]["record"]["$type"])
        
            #print(json.dumps(message["commit"]["record"]["text"], indent=2))
        if is_suitable_post(message):
           print('s')
        show_fields(message)
           #print(json.dumps(message, indent=2))
      except websockets.ConnectionClosed as e:
        print(f"Connection closed: {e}")
        break
      except Exception as e:
        print(f"Error: {e}")
        print(json.dumps(message))

asyncio.get_event_loop().run_until_complete(listen_to_websocket())