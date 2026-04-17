
from atproto import FirehoseSubscribeReposClient, CAR, CID, firehose_models

# from atproto_core import cbor
from datetime import datetime, timezone, timedelta
import numpy

# import python_test_2
import sqlite3
import os
import argparse
import time
import random
import langid
import json
import re

import requests

metadata = {
    'called' : 0,
    'langid_english' : 0,
    'langid_not_english' : 0,
    'english_tag' : 0,
    'not_english_tag' : 0,
    'valid_time_count' : 0,
    'invalid_time' : 0,
    'no_reply' : 0,
    'reply' : 0,
    'no_embed' : 0,
    'embed' : 0,
    'invalid_repost_time' : 0
    }

POST_MAX_TRACKING_TIME_SEC = 24 * 60 * 60
MAX_ACTUAL_TIME_DIFFERENCE_SEC = 120
TRACKING_TIME_SEC = 60 * 60 * 24 * 15   + 60 * 60
SAMPLE_RATE = 1
database_name = datetime.now().strftime('%m-%d-%Y_%H-%M')

os.makedirs('./data', exist_ok=True)
database = sqlite3.connect(f"./data/{database_name}.db")
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
                post_uri TEXT NOT NULL UNIQUE, 
                post_time TEXT NOT NULL, 
                post_text TEXT,
                delete_time TEXT,
                is_news BOOLEAN,
                PRIMARY KEY(post_cid))""")
    
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS reposts(
                post_cid TEXT,
                post_uri TEXT, 
                repost_uri TEXT NOT NULL UNIQUE,
                repost_time TEXT,
                delete_time TEXT, 
                PRIMARY KEY(repost_uri),
                FOREIGN KEY(post_cid) 
                REFERENCES posts(post_cid) 
                ON DELETE CASCADE)"""
    )


def delete_tables(cursor):
    cursor.execute("DROP TABLE IF EXISTS posts")
    cursor.execute("DROP TABLE IF EXISTS reposts")


def insert_post(cursor, post_cid, post_uri, post_time, post_text, is_news):

    try:
        cursor.execute(
            "INSERT INTO posts (post_cid, post_uri, post_time, post_text, is_news) VALUES (?, ?, ?, ?, ?)",
            (post_cid, post_uri, post_time, post_text, is_news),
        )
    except sqlite3.IntegrityError:
        print(f"post with cid {post_cid} already exists")
        pass

def delete_post(cursor, post_uri, delete_time):

    cursor.execute(
        "UPDATE posts SET delete_time = ? WHERE post_uri = ?", 
        (delete_time, post_uri)
    )

def insert_repost(
    cursor: sqlite3.Cursor, post_cid, post_uri, repost_uri, repost_time: datetime
):
    cursor.execute("SELECT post_time FROM posts WHERE post_cid = ? ", (post_cid,))
    post_time = cursor.fetchone()
    if post_time:
        post_time = post_time[0]
        seconds_diff = (repost_time - datetime.fromisoformat(post_time)).total_seconds()
        if seconds_diff <= POST_MAX_TRACKING_TIME_SEC:
            cursor.execute(
                "INSERT INTO reposts (post_cid, post_uri, repost_uri, repost_time) VALUES (?, ?, ?, ?)",
                (post_cid, post_uri, repost_uri, repost_time),
            )
        else:
            raise TooLateException(f"{repost_time} Too far after {post_time}")
    else:
        raise PostNotTrackedException()

def delete_repost(cursor, repost_uri, delete_time):
    cursor.execute(
        "UPDATE reposts SET delete_time = ? WHERE repost_uri = ?", 
        (delete_time, repost_uri)
    )


def commit_inserts(database, last_seq):
    database.commit()
    with open("last_seq.txt", "w") as output_file:
        output_file.write(str(last_seq))


def main(last_seq):
    client = FirehoseSubscribeReposClient(params={"cursor": last_seq})

    execute_counter = 0
    last_event_seq = last_seq

    cursor = database.cursor()
    news_dids = json.load(open('news_dids.json', 'r'))

    #delete_tables(cursor)
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
        path = op.get("path")
        post_uri = f"at://{repo}/{path}"
        
        if action != "create":
            if action == "delete":
                
                delete_post(cursor, post_uri, datetime.now(timezone.utc))
                return True
            
        else:
            try:
                news_post = False

                if repo in news_dids:
                    news_post = True

                raw_post_cid = op.get("cid")
                post_cid = str(CID.decode(raw_post_cid))
                post_block = carFile.blocks[post_cid]
                embed = post_block.get("embed")
                reply = post_block.get("reply")
                langs = post_block.get("langs")
                
                if reply != None:
                    metadata['reply'] += 1
                    return False
                metadata['no_reply'] += 1

                if news_post == False:
                    if embed != None:
                        metadata['embed'] += 1
                        return False
                    metadata['no_embed'] += 1
                
                post_time = generalize_time(post_block.get("createdAt"))

                if not valid_time(post_time):
                    metadata['invalid_time'] += 1
                    return False
                metadata['valid_time_count'] += 1
                
                post_text = post_block.get("text")
                

                if post_text == None:
                    print(f"None: {post_block}")
                    return False
                
                post_text = re.sub(r'@[\w.]+', '@user', post_text)
                
                if langs == None or "en" in langs:
                    metadata['english_tag'] += 1
                    if langid.classify(post_text)[0] == 'en':
                        metadata['langid_english'] += 1

                    else:
                        metadata['langid_not_english'] += 1
                        return False
                else:   
                    metadata['not_english_tag'] += 1
                    return False

                if random.random() <= SAMPLE_RATE or news_post == True:
                    insert_post(cursor, post_cid, post_uri, post_time, post_text, news_post)
                    return True
                
            except Exception as e:
                print(e)
                pass
            return False

    def handle_repost(op, carFile, repo):

        action = op.get("action", "")
        path = op.get("path")
        repost_uri = f"at://{repo}/{path}"  

        if action != "create":
            if action == "delete":     
                
                delete_repost(cursor, repost_uri, datetime.now(timezone.utc))
                return True

        else:
            try:
                raw_repost_cid = op.get("cid")
                repost_cid = str(CID.decode(raw_repost_cid))

                repost_block = carFile.blocks[repost_cid]
                subject = repost_block.get("subject")

                repost_time = generalize_time(repost_block.get("createdAt"))
                post_cid = subject.get("cid")
                post_uri = subject.get("uri")

                if valid_time(repost_time) == False:
                    repost_time = datetime.now(timezone.utc)
                    metadata['invalid_repost_time'] += 1

                if post_cid == None:
                    print("repost_cid not pointing to anything")
                else:

                    try:
                        # python_test_2.getPosts([post_uri])
                        insert_repost(
                            cursor, post_cid, post_uri, repost_uri, repost_time
                        )
                        return True

                    except (PostNotTrackedException, TooLateException):
                        pass

                    except Exception as e:
                        print(e)

            except Exception as e:
                print(e)
                pass

            return False
        
    def error_handler(event):
        print(f"Error: {event}")

    def listen_to_websocket(event):
        nonlocal execute_counter
        nonlocal last_event_seq
        nonlocal last_seq

        metadata['called'] += 1

        if event.header.op != 1:
            return

        seq = event.body.get("seq")
        if last_event_seq and seq != last_event_seq + 1:
            print(f'seq {seq} mismatches last_seq {last_event_seq}')
        last_event_seq = seq

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
                    if execute_counter % 1000 == 0:
                        commit_inserts(database, seq)

            elif opType.startswith("app.bsky.feed.post/"):

                
                if handle_post(op, carFile, repo):
                    execute_counter += 1
                    if execute_counter % 1000 == 0:
                        commit_inserts(database, seq)

            else:
                continue

        
        last_seq = seq

    client.on_repo_commit = listen_to_websocket

    client.start(listen_to_websocket, error_handler)

    print("Stopping")

    client.stop()


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
        main(None)
    except (Exception, KeyboardInterrupt) as e:
        print(f"Error {e}")
    finally:
        database.commit()
        database.close()
        with open(f"./data/{database_name}_metadata.json", "w") as f:
            json.dump(metadata, f)
