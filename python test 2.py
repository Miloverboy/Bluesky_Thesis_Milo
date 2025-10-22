import requests
import pandas as pd
#from atproto import Client
#import json
from langdetect import detect, detect_langs
from langdetect.lang_detect_exception import LangDetectException
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from  roberta_sentiment import roberta_sentiment
import csv

data = pd.read_json(r"2025-09-08_posts.json", lines=True, nrows=100)

uri_batches = []
uris = []

for index, row in data.iterrows():
    if row['$type'] == 'app.bsky.feed.post':
        #print(row)
        #print(str(index) + ' : ' + row['uri'] + ' : ' + row['text'] + '\n')
        uris.append(row['uri'])
    if len(uris) == 25:
        uri_batches.append(uris)
        uris = []

#print(uri_batches)

print(len(uri_batches[0]))

url = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"

repostedCount = 0
unrepostedCount = 0
noTextCount = 0
notEnglishCount = 0
failedText = [[]] # store text on which the language detector fails

analyzer = SentimentIntensityAnalyzer()

with open('sentiment_results.csv', 'w', encoding="utf-8", newline='') as outputFile:

    writer = csv.writer(outputFile, quoting=csv.QUOTE_ALL)

    for uris in uri_batches :

        # Make the GET request with query parameters
        response = requests.get(url, params={"uris": uris})

        # Check if it worked
        if response.status_code == 200:
            data = response.json()  # Parse the JSON response
            posts = data.get("posts", [])

            print(len(posts))
            
            # Print each post’s basic info
            for post in posts:
                author = post["author"]["handle"]
                text = post["record"].get("text", "")
                reposts = post["repostCount"]
                if any(map(str.isalpha, text)): #check if there is text in post   
                    try:
                        if(detect(text) == 'en'): # detect language
                            sentiment_2 = roberta_sentiment(text)
                            sentiment = analyzer.polarity_scores(text).get('compound')
                            #print(f"{author}: \n{text} ({reposts}) \nsentiment: {sentiment.get('compound')}\n")
                            print(f"{text}\n Vader sentiment : {sentiment} , Roberta sentiment : {sentiment_2}")
                            writer.writerow([text,sentiment,sentiment_2])
                            if (reposts > 0) : 
                                repostedCount += 1
                            else : 
                                unrepostedCount += 1
                    except LangDetectException as e:
                        failedText.append(text)
                    else:
                        notEnglishCount += 1
                else:
                    noTextCount += 1
        else:
            print(f"Error {response.status_code}: {response.text}")

    print(f" unreposted: {unrepostedCount} \n reposted: {repostedCount} \n no text: {noTextCount} \n other languages: {notEnglishCount}")

    if (any(failedText)) : print(failedText)
