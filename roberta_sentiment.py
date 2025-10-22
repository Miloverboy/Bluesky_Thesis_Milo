from transformers import AutoModelForSequenceClassification
from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax
import time

# Most part of this code is directly copied from the example on https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest

# Significant change is that the sentiment score is given by the probability that sentiment is positive minus prob. text is negative . should be reworked

MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)
# PT
model = AutoModelForSequenceClassification.from_pretrained(MODEL)

def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

def roberta_sentiment(text):
    
    #model.save_pretrained(MODEL)

    text = preprocess(text)
    encoded_input = tokenizer(text, return_tensors='pt')
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    # # TF
    # model = TFAutoModelForSequenceClassification.from_pretrained(MODEL)
    # model.save_pretrained(MODEL)
    # text = "Covid cases are increasing fast!"
    # encoded_input = tokenizer(text, return_tensors='tf')
    # output = model(encoded_input)
    # scores = output[0][0].numpy()
    # scores = softmax(scores)
    # Print labels and scores
    ranking = np.argsort(scores)
    ranking = ranking[::-1]
    for i in range(scores.shape[0]):
        l = config.id2label[ranking[i]]
        s = scores[ranking[i]]
        #print(scores)
        sentiment = scores[2] - scores[0]
        #return f"{i+1}) {l} {np.round(float(s), 4)}"
        return sentiment

text = 'Billionaires and their private equity funds are gonna snatch up great farmland at bargain prices! Magats voted for their own ruin in Arkansas and other farm areas.'   
text2 =  'An Alabama man was arrested after he reportedly traveled to California, where he was arrested for threatening a Los Angeles area church in late August, according to authorities.'
print(roberta_sentiment(text))
print(roberta_sentiment(text2))

