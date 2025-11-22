import asyncio
from atproto import AsyncFirehoseSubscribeReposClient, CAR, CID
#from atproto_core import cbor
from datetime import datetime, timezone
import numpy
# import python_test_2
import sqlite3
import os
import argparse


uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

def create_tables(cursor):
    cursor.execute('CREATE TABLE IF NOT EXISTS posts(cid, uri)')
    cursor.execute('CREATE TABLE IF NOT EXISTS reposts(post_cid, user_id, time, FOREIGN KEY (post_cid) REFERENCES posts (cid))')

def delete_tables(cursor):
    cursor.execute('DROP TABLE IF EXISTS posts')
    cursor.execute('DROP TABLE IF EXISTS reposts')

def insert_post(cursor, post_cid, post_uri):
    cursor.execute('INSERT INTO posts (cid, uri) VALUES (?, ?)', (post_cid, post_uri))

def insert_repost(cursor, post_cid, user_id, time):
    cursor.execute('INSERT INTO reposts (post_cid, user_id, time) VALUES (?, ?, ?)', (post_cid, user_id, time))

def commit_inserts(database, last_seq):
    database.commit()
    with open('last_seq.txt', 'w') as output_file:
        output_file.write(str(last_seq))

async def main(last_seq):
    client = AsyncFirehoseSubscribeReposClient(params = {'cursor' : last_seq})
    useless = 0
    processed = 0
    called = 0
    times = []
    posts_to_store = set()
    isMatch = 0
    last_seq = None
    execute_counter = 0

    database = sqlite3.connect('genericFirehoseResults.db')
    cursor = database.cursor()
    
    delete_tables(cursor)
    create_tables(cursor)


    async def handle_post(op):
        nonlocal posts_to_store
        nonlocal execute_counter
        action = op.get("action","")
        if action != "create":
            #print(action)
            i = 0
        else:
            if len(posts_to_store) > 10000:
                return
            elif len(posts_to_store) % 100 == 0:
                print(f'length: {len(posts_to_store)}')
                
            post_uri = op.get('path').split(r'/')[1]
            raw_post_cid = op.get('cid')
            post_cid = str(CID.decode(raw_post_cid))   
            #print(f'post: {post_cid}')
            #print(op)
            if True:
                posts_to_store.add(post_uri)
                insert_post(cursor, post_cid, post_uri)
                execute_counter += 1
                #print(f'added {post_uri}')


    async def handle_repost(op, carFile):
        nonlocal isMatch
        nonlocal posts_to_store
        nonlocal execute_counter


        
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
            post_cid = str(CID.decode(raw_post_cid))
            for key in carFile.blocks.keys():
                if str(key) == post_cid:
                    repostedUriFull = carFile.blocks[key].get('subject').get('uri')
                    repostedUri = repostedUriFull.split('/')[-1]
            
          
            if repostedUri in posts_to_store:
                #print('match!')
                isMatch += 1
                insert_repost(cursor, post_cid, 1, datetime.now(timezone.utc))
                execute_counter += 1
                #print(repostedUri)

                #python_test_2.getPosts([repostedUriFull])
            #print(f'checked {repostedUri}')
            
            

            
                
    
    async def listen_to_websocket(event):
        nonlocal posts_to_store
        nonlocal called
        nonlocal processed
        nonlocal times
        nonlocal useless
        nonlocal execute_counter


        seq = event.body.get('seq')   

        called += 1

        ops = event.body.get("ops", [])
        #print(event)
        #print (event.body.get('blocks'))
        
        for op in ops:
            opType = op.get("path", "")
            if opType.startswith("app.bsky.feed.repost") :
                
                carFile = CAR.from_bytes(event.body['blocks'])
                await handle_repost(op, carFile)
              
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
        if execute_counter % 100 == 0:
            commit_inserts(database, seq)
    
    async def Match():
        nonlocal isMatch
        while isMatch < 500:
            await asyncio.sleep(5)
        print(isMatch)
        return
        

    client.on_repo_commit = listen_to_websocket
    
    task = asyncio.create_task(client.start(listen_to_websocket))
    
    #await asyncio.sleep(1)
    await Match()
    await client.stop()
    try :
        await task
    except Exception as e:
        print(f'Error {e}')
    if len(times)>0:
      print(f"first: {times[0]}, last: {times[len(times)-1]}, average: {numpy.average(times)}")
      print(f"called: {called}, processed: {processed}, useless: {useless}")
      print(times[::500])
    #print(posts_to_store)

parser = argparse.ArgumentParser('firehose_listener.py',
                                 description= 'listen to firehose and store suitable new \'create post events\' in database. Then store reposts that relate to the stored posts',
                                 epilog = 'test')
parser.add_argument('-last_seq', required=False)
args = parser.parse_args()
last_seq = args.last_seq

asyncio.run(main(last_seq))




#asyncio.get_event_loop().run_until_complete(listen_to_websocket())