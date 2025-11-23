import requests
import pandas as pd
#from atproto import Client
import json
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
    if len(uris) == 25: # 1 request can only contain 25 uris
        uri_batches.append(uris)
        uris = []

def getPosts(uri_batches):

    # uri_batches = [["at://did:plc:2ucib6krqt5gneltbwa3d6t7/app.bsky.feed.post/3m65uekifas2c"]]
    #print('t')

    url = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"

    repostedCount = 0
    unrepostedCount = 0
    noTextCount = 0
    notEnglishCount = 0
    failedText = [[]]   # store text on which the language detector fails. 
                        # these are rare cases in which there is text, but the text fails the language detector

    analyzer = SentimentIntensityAnalyzer() # Vader sentiment analyser



    with open('sentiment_results.csv', 'w', encoding="utf-8", newline='') as outputFile:

        writer = csv.writer(outputFile)

        for uris in uri_batches :

            response = requests.get(url, params={"uris": uris})

            

            if response.status_code == 200:
                data = response.json()
                posts = data.get("posts", [])
                print(data)

                #print(json.dumps(data, indent = 2))

                for post in posts:
                    author = post["author"]["handle"]
                    text = post["record"].get("text", "")
                    reposts = post["repostCount"]
                    #print(f'text: {text}')

                    try:
                        if not any(map(str.isalpha, text)): #check if there is text in post
                            noTextCount += 1
                        elif (detect(text) != 'en'): # detect language (should probably be changed to use a different tool)
                            notEnglishCount += 1

                        else:
                            sentiment_2 = roberta_sentiment(text) # Roberta sentiment is AI and works better for text requiring some context knowledge
                            sentiment = analyzer.polarity_scores(text).get('compound') # Vader sentiment is simple and not really good
                            #print(f"{author}: \n{text} ({reposts}) \nsentiment: {sentiment.get('compound')}\n")
                            print(f"{text}\n Vader sentiment : {sentiment} , Roberta sentiment : {sentiment_2}")
                            writer.writerow([text.replace('\n', '\\n'),sentiment,sentiment_2]) 

                            if (reposts > 0) : 
                                repostedCount += 1
                            else : 
                                unrepostedCount += 1

                    except LangDetectException as e: # Sometimes a text containing unusual letters fails the language detection library
                        failedText.append(text) 
                        
                        
            else:
                print(f"Error {response.status_code}: {response.text}")
                print(uris)

        #print(f" unreposted: {unrepostedCount} \n reposted: {repostedCount} \n no text: {noTextCount} \n other languages: {notEnglishCount}")

        if (any(failedText)) : print(failedText)

#getPosts(uri_batches)
