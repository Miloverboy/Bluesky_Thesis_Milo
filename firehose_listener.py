import asyncio
from atproto import AsyncFirehoseSubscribeReposClient, CAR, CID, firehose_models

# from atproto_core import cbor
from datetime import datetime, timezone, timedelta
import numpy

# import python_test_2
import sqlite3
import os
import argparse
import time
import random

REPOST_MAX_TIME_FRAME_SEC = 24 * 60 * 60
MAX_ACTUAL_TIME_DIFFERENCE_SEC = 120
TRACKING_TIME_SEC = 60 * 60 * 24 * 15
SAMPLE_RATE = 1

os.makedirs('./data', exist_ok=True)
database = sqlite3.connect(f"./data/{datetime.now().strftime('%m-%d-%Y_%H-%M')}.db")
database.execute("PRAGMA journal_mode=WAL;")
database.execute("PRAGMA synchronous=NORMAL;")
# uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


class TooLateException(Exception):
    pass


class PostNotTrackedException(Exception):
    pass


def create_tables(cursor: sqlite3.Cursor):
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS posts(
                   post_cid TEXT NOT NULL UNIQUE,
                   post_uri TEXT NOT NULL, 
                   post_time TEXT NOT NULL, 
                   post_text TEXT, 
                   PRIMARY KEY(post_cid))"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS reposts(
                   post_cid TEXT,
                   post_uri TEXT, 
                   reposter_did INTEGER, 
                   repost_time TEXT, 
                   FOREIGN KEY(post_cid) 
                   REFERENCES posts(post_cid) 
                   ON DELETE CASCADE)"""
    )


def delete_tables(cursor):
    cursor.execute("DROP TABLE IF EXISTS posts")
    cursor.execute("DROP TABLE IF EXISTS reposts")


def insert_post(cursor, post_cid, post_uri, post_time, post_text):

    cursor.execute(
        "INSERT INTO posts (post_cid, post_uri, post_time, post_text) VALUES (?, ?, ?, ?)",
        (post_cid, post_uri, post_time, post_text),
    )


def insert_repost(
    cursor: sqlite3.Cursor, post_cid, post_uri, reposter_did, repost_time: datetime
):
    cursor.execute("SELECT post_time FROM posts WHERE post_cid = ? ", (post_cid,))
    post_time = cursor.fetchone()
    if post_time:
        post_time = post_time[0]
        seconds_diff = (repost_time - datetime.fromisoformat(post_time)).total_seconds()
        if seconds_diff <= REPOST_MAX_TIME_FRAME_SEC:
            cursor.execute(
                "INSERT INTO reposts (post_cid, post_uri, reposter_did, repost_time) VALUES (?, ?, ?, ?)",
                (post_cid, post_uri, reposter_did, repost_time),
            )
        else:
            raise TooLateException(f"{repost_time} Too far after {post_time}")
    else:
        raise PostNotTrackedException()


def commit_inserts(database, last_seq):
    database.commit()
    with open("last_seq.txt", "w") as output_file:
        output_file.write(str(last_seq))

async def monitor_loop():
    while True:
        start = asyncio.get_running_loop().time()
        await asyncio.sleep(10)
        end = asyncio.get_running_loop().time()
        lag = end - start - 10
        print(f"Event loop lag: {lag:.4f}s, Total tasks: {len(asyncio.all_tasks())}")

async def main(last_seq):
    client = AsyncFirehoseSubscribeReposClient(params={"cursor": last_seq})
    useless = 0
    processed = 0
    called = 0
    times = []
    isMatch = 0
    execute_counter = 0
    last_event_seq = last_seq

    cursor = database.cursor()

    delete_tables(cursor)
    create_tables(cursor)

    def generalize_time(time_str):
        if time_str.endswith("Z"):
            time_str = time_str[:-1] + "+00:00"
        time_dt = datetime.fromisoformat(time_str)
        return time_dt.astimezone(timezone.utc)

    def valid_time(time_dt):

        actual_time = datetime.now(timezone.utc)
        return (
            abs((actual_time - time_dt).total_seconds())
            <= MAX_ACTUAL_TIME_DIFFERENCE_SEC
        )

    def handle_post(op, carFile, repo):
        action = op.get("action", "")
        if action != "create":
            i = 0
        else:
            try:
                path = op.get("path")
                post_uri = f"at://{repo}/{path}"
                raw_post_cid = op.get("cid")
                post_cid = str(CID.decode(raw_post_cid))
                post_block = carFile.blocks[post_cid]
                embed = post_block.get("embed")
                reply = post_block.get("reply")
                langs = post_block.get("langs")
                if langs == None or "en" not in langs or embed != None or reply != None:
                    return False
                post_time = generalize_time(post_block.get("createdAt"))
                post_text = post_block.get("text")

                if post_text == None:
                    print(f"None: {post_block}")
                    return False
                if not valid_time(post_time):
                    print('time')
                    return False

                if embed != None:
                    print('embed')
                    return

                if random.random() <= SAMPLE_RATE:
                    #print('saving')
                    insert_post(cursor, post_cid, post_uri, post_time, post_text)
                    return True
            except Exception as e:
                pass
            return False

    def handle_repost(op, carFile, repo):

        action = op.get("action", "")

        if action != "create":
            i = 0

        else:
            try:
                raw_repost_cid = op.get("cid")
                repost_cid = str(CID.decode(raw_repost_cid))

                subject = carFile.blocks[repost_cid].get("subject")

                post_cid = subject.get("cid")
                post_uri = subject.get("uri")

                if post_cid == None:
                    print("repost_cid not pointing to anything")
                else:

                    try:
                        # python_test_2.getPosts([post_uri])
                        insert_repost(
                            cursor, post_cid, post_uri, repo, datetime.now(timezone.utc)
                        )
                        return True

                    except PostNotTrackedException:
                        pass

                    except Exception as e:
                        print(e)
            except Exception as e:
                pass

            return False

    async def listen_to_websocket(event):
        nonlocal called
        nonlocal processed
        nonlocal times
        nonlocal useless
        nonlocal execute_counter
        nonlocal last_event_seq
        nonlocal last_seq

        if event.header.op != 1:
            #print(event)
            return

        start = time.perf_counter()

        seq = event.body.get("seq")
        if last_event_seq and seq != last_event_seq + 1:
            print(f'seq {seq} mismatches last_seq {last_event_seq}')
        last_event_seq = seq

        called += 1
        # print(event)

        repo = event.body.get("repo")
        ops = event.body.get("ops", [])

        if len(ops) == 0:
            return

        try:
            carFile = CAR.from_bytes(event.body["blocks"])
        except Exception as e:
            pass

        for op in ops:

            opType = op.get("path", "")
            if opType.startswith("app.bsky.feed.repost/"):

                if handle_repost(op, carFile, repo):
                    execute_counter += 1

            elif opType.startswith("app.bsky.feed.post/"):

                
                if handle_post(op, carFile, repo):
                    execute_counter += 1

            else:
                useless += 1
                continue
                print(type)
            event_time = event.body.get("time")
            if event_time != None:
                event_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
                taken_time = datetime.now(timezone.utc) - event_time
            else:
                print('no time')

            processed += 1
        if execute_counter % 500 == 1:
            commit_inserts(database, seq)
        last_seq = seq
        end = time.perf_counter()
        elapsed = end - start
        times.append(elapsed)
        # print(f'time: {elapsed}')

    async def Match():
        nonlocal isMatch
        while isMatch < 5000:
            await asyncio.sleep(2)
            # isMatch += 1
        return

    client.on_repo_commit = listen_to_websocket

    task = asyncio.create_task(client.start(listen_to_websocket))

    asyncio.create_task(monitor_loop())
    await asyncio.sleep(TRACKING_TIME_SEC)
    print("Stopping")
    # await Match()
    #commit_inserts(database, 2)

    await client.stop()
    
    if len(times) > 0:
        print(
            f"first: {times[0]}, last: {times[len(times)-1]}, average: {numpy.average(times)}"
        )
        print(f"called: {called}, processed: {processed}, useless: {useless}")
        #print(times[::500])


parser = argparse.ArgumentParser(
    "firehose_listener.py",
    description="listen to firehose and store suitable new 'create post events' in database. Then store reposts that relate to the stored posts",
    epilog="test",
)
parser.add_argument("-last_seq", required=False)
args = parser.parse_args()
last_seq = args.last_seq


if __name__ == '__main__':
    try:
        asyncio.run(main(last_seq))
    except (Exception, KeyboardInterrupt) as e:
        print(f"Error {e}")
        database.commit()
        database.close()
        


# asyncio.get_event_loop().run_until_complete(listen_to_websocket())
