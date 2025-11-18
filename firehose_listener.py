import asyncio
import websockets
import json
import re
import time
from atproto import AsyncFirehoseSubscribeReposClient, CAR, CID
from atproto_core import cbor
from datetime import datetime, timezone
import numpy
import dag_cbor
import python_test_2


uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
url = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"



async def main():
    client = AsyncFirehoseSubscribeReposClient()
    useless = 0
    processed = 0
    called = 0
    times = []
    posts_to_store = set()
    isMatch = 0


    async def handle_post(op):
        nonlocal posts_to_store
        action = op.get("action","")
        if action != "create":
            #print(action)
            i = 0
        else:
            if len(posts_to_store) > 500:
                return
            elif len(posts_to_store) % 1000 == 0:
                print(f'length: {len(posts_to_store)}')
            post_uri = op.get('path').split(r'/')[1]   
            #print(f'post: {raw_post_cid}')
            #print(op)
            if True:
                posts_to_store.add(post_uri)
                #print(f'added {post_uri}')


    async def handle_repost(op, carFile):
        nonlocal isMatch
        nonlocal posts_to_store


        
        action = op.get("action","")      

        

        #await client.stop()
        #print(raw_post_cid)
        #post_cid = CID.decode(raw_post_cid)
        #print(f'repost: {raw_post_cid}')
        if action != "create":
            i = 0
            #print(op)
        else: 
            raw_post_cid = op.get('cid')
            post_cid = CID.decode(raw_post_cid)
            for key in carFile.blocks.keys():
                if key == post_cid:
                    repostedUriFull = carFile.blocks[key].get('subject').get('uri')
                    repostedUri = repostedUriFull.split('/')[-1]
                    
                    break
            
          
            if repostedUri in posts_to_store:
                print('match!')
                isMatch += 1
                print(repostedUri)
                python_test_2.getPosts([repostedUriFull])
            #print(f'checked {repostedUri}')
            
            

            
                
    
    async def listen_to_websocket(event):
        nonlocal posts_to_store
        nonlocal called
        nonlocal processed
        nonlocal times
        nonlocal useless

        called += 1

        ops = event.body.get("ops", [])
        #print(event)
        #print (event.body.get('blocks'))
        
        for op in ops:
            opType = op.get("path", "")
            if opType.startswith("app.bsky.feed.repost") :
                
                carFile = CAR.from_bytes(event.body['blocks'])
                #print(await dag_cbor.decode(event.body['blocks']))
                #print(list(carFile.blocks.keys()))
                #print(cbor.decode_dag_multi(event.body['blocks']))
                
                #print(f'cid: {post_cid}')
                #print('called')
                await handle_repost(op, carFile)
                break
                #print(event)
                #print(await dag_cbor.decode(event.body['blocks']))
                #await client.stop()
                reader = CAR.from_bytes(event.body["blocks"])
                blocks = reader.blocks#.get(op.get('cid'))
                
                for block in blocks.values():
                  #print(f'blockie: {block} \n')
                  for e in block.get('e', []):
                      e_cid = CID.decode(e.get('v'))
                      if post_cid == e_cid:
                        print(e)
                      #print(key_str)
                      #print(post_cid)
                      #if str(post_cid) in key_str:
                          #original_post = e.get("v")
                          #print(original_post)
                  #record = dag_cbor.decode(record_bytes)
                  #print(record)
                
                #for CBOR_block in event.body['blocks']:
                    #print(CBOR_block)
                    #block = await dag_cbor.decode(CBOR_block)
                
                    

                await client.stop()
              
            elif opType.startswith("app.bsky.feed.post") :
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
    
    async def Match():
        nonlocal isMatch
        while isMatch < 50:
            await asyncio.sleep(5)
        print(isMatch)
        return
        

    client.on_repo_commit = listen_to_websocket
    task = asyncio.create_task(client.start(listen_to_websocket))
    #await asyncio.sleep(1)
    await Match()
    await client.stop()
    
    await task
    if len(times)>0:
      print(f"first: {times[0]}, last: {times[len(times)-1]}, average: {numpy.average(times)}")
      print(f"called: {called}, processed: {processed}, useless: {useless}")
      print(times[::500])
    #print(posts_to_store)

asyncio.run(main())




#asyncio.get_event_loop().run_until_complete(listen_to_websocket())