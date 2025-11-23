import asyncio
from atproto import AsyncFirehoseSubscribeReposClient, CAR, CID
#from atproto_core import cbor
from datetime import datetime, timezone
import numpy
#import python_test_2
import sqlite3
import os
import argparse
import time


uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

def create_tables(cursor: sqlite3.Cursor):
    cursor.execute('PRAGMA foreign_keys = ON')
    cursor.execute('CREATE TABLE IF NOT EXISTS posts(cid TEXT, uri TEXT NOT NULL UNIQUE, PRIMARY KEY(uri))')
    cursor.execute('CREATE TABLE IF NOT EXISTS reposts(post_cid TEXT, post_uri TEXT, reposter_did INTEGER, time, FOREIGN KEY(post_uri) REFERENCES posts(uri) ON DELETE CASCADE)')

def delete_tables(cursor):
    cursor.execute('DROP TABLE IF EXISTS posts')
    cursor.execute('DROP TABLE IF EXISTS reposts')

def insert_post(cursor, post_cid, post_uri):
    # print('i')
    # print(post_cid)
    cursor.execute('INSERT INTO posts (cid, uri) VALUES (?, ?)', (post_cid, post_uri))

def insert_repost(cursor, post_cid, post_uri, reposter_did, time):    
    # print(post_cid)
    
    cursor.execute('INSERT INTO reposts (post_cid, post_uri, reposter_did, time) VALUES (?, ?, ?, ?)', (post_cid, post_uri, reposter_did, time))
    print('repost')


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


    async def handle_post(op, repo):
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
                

            path = op.get('path')#.split(r'/')[1]
            post_uri = f'at://{repo}/{path}'
            raw_post_cid = op.get('cid')
            post_cid = str(CID.decode(raw_post_cid))

            #await client.stop()
            #print(post_cid)
            #print(post_uri)

            #iii = carFile.blocks[post_cid]
            #print(iii)
            
            #python_test_2.getPosts([iii.get('uri')])   
            #print(f'post: {post_cid}')
            #print(op)
            if True:
                posts_to_store.add(post_uri)
                insert_post(cursor, post_cid, post_uri)
                execute_counter += 1
                #print(f'added {post_uri}')


    async def handle_repost(op, carFile, repo):
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
            raw_repost_cid = op.get('cid')
            repost_cid = str(CID.decode(raw_repost_cid))
            # print(op)
            # print(carFile.blocks.keys())

            subject = carFile.blocks[repost_cid].get('subject')

            post_cid = subject.get('cid')
            post_uri = subject.get('uri')
            
            #print(post_uri)
            # print(post_cid)
            if post_uri in posts_to_store:
                        print(post_uri)
                        print(post_cid)
            if post_cid == None:
                print('repost_cid not pointing to anything')
            else: 
                #await client.stop()


                try:
                    #python_test_2.getPosts([post_uri])
                    insert_repost(cursor, post_cid, post_uri, repo, datetime.now(timezone.utc))
                    isMatch += 1
                    execute_counter += 1
                    
                except Exception as e:
                        #print(e)
                        pass

            return
            
            for key in carFile.blocks.keys():
                print(f'{key} -----> {carFile.blocks[key]} \n')
                # print(carFile.blocks[key])
                if str(key) == post_cid:
                    try:
                        repostedUriFull = carFile.blocks[key].get('subject').get('uri')
                        print(repostedUriFull)
                        await client.stop()
                        insert_repost(cursor, post_cid, 1, datetime.now(timezone.utc))
                        isMatch += 1
                        execute_counter += 1
                    except Exception as e:
                        pass
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

        start = time.perf_counter()

        seq = event.body.get('seq')   

        called += 1
        #print(event)

        
        repo = event.body.get('repo')
        ops = event.body.get("ops", [])
        #print(event)
        #print (event.body.get('blocks'))
        carFile = None
        
        for op in ops:
            opType = op.get("path", "")
            if opType.startswith("app.bsky.feed.repost") :
                
                if carFile == None:
                    carFile = CAR.from_bytes(event.body['blocks'])
                
                await handle_repost(op, carFile, repo)
              
            elif opType.startswith("app.bsky.feed.post") :
                
                

                await handle_post(op, repo)
                
            else: 
                useless += 1
                return
                print(type)
            event_time = event.body.get("time")
            if event_time != None: 
                event_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
                taken_time = datetime.now(timezone.utc) - event_time
            else:
                print(event.body)
            times.append(taken_time.total_seconds())
            processed += 1
        if execute_counter % 100 == 1:
            commit_inserts(database, seq)
        end = time.perf_counter()
        elapsed = end - start
        #print(f'time: {elapsed}')
    
    async def Match():
        nonlocal isMatch
        while isMatch < 100:
            await asyncio.sleep(2)
            #isMatch += 1
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