import asyncio
import websockets
import json
import re
import time
from atproto import AsyncFirehoseSubscribeReposClient
from atproto_core import cbor
from datetime import datetime, timezone
import numpy


uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

async def main():
    client = AsyncFirehoseSubscribeReposClient()
    useless = 0
    processed = 0
    called = 0
    times = []
    posts_to_store = []



    async def handle_post(op):
        nonlocal posts_to_store
        action = op.get("action","")
        if action != "create":
            #print(action)
            i = 0
        else:
            #print(op)
            if True:
                post_id = op.get("path").split("/")[-1]
                posts_to_store.append(post_id)


    async def handle_repost(op):
        nonlocal posts_to_store
        action = op.get("action","")
        if action != "create":
            i = 0
            print(op)

            
                
    
    async def listen_to_websocket(event):
        nonlocal posts_to_store
        nonlocal called
        nonlocal processed
        nonlocal times
        nonlocal useless

        called += 1

        ops = event.body.get("ops", [])
        for op in ops:
            type = op.get("path", "")
            if type.startswith("app.bsky.feed.repost") :
                await handle_repost(op)
                for CBOR_block in event.body['blocks']:
                    block = await cbor.decode_dag(CBOR_block)
                    print(block)

                await client.stop()
              
            elif type.startswith("app.bsky.feed.post") :
                await handle_post(op)
                
            else: 
                useless += 1
                return
                print(type)
            time = event.body.get("time")
            if time != None: 
                event_time = datetime.fromisoformat(time.replace("Z", "+00:00"))
                taken_time = datetime.now(timezone.utc) - event_time
            else:
                print(event.body)
            times.append(taken_time.total_seconds())
            processed += 1
        

    client.on_repo_commit = listen_to_websocket
    task = asyncio.create_task(client.start(listen_to_websocket))
    await asyncio.sleep(10)
    await client.stop()
    await task
    print(f"first: {times[0]}, last: {times[len(times)-1]}, average: {numpy.average(times)}")
    print(f"called: {called}, processed: {processed}, useless: {useless}")
    #print(posts_to_store)

asyncio.run(main())



'''
def is_suitable_post(message):
    if "commit" not in message or "record" not in message["commit"] or message["commit"]["record"]["$type"] != "app.bsky.feed.post":
      print('not a post')
      return False
    else:
       record = message["commit"]["record"]


    if "langs" not in record: # remove
      print('not eng')
      return False
    elif "reply" in record: # Check if post is a reply to another post. We want to filter those out
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
  '''    

'''
async def listen_to_websocket():
  async with websockets.connect(uri) as websocket:
    for i in range(101):
      try:
        response = await websocket.recv()
        message = json.loads(response)
        if message['$type'] == 'app.bsky.feed.post':
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
'''





#asyncio.get_event_loop().run_until_complete(listen_to_websocket())