import json
import pandas as pd
import matplotlib.pyplot as plt


tweets_data = []

tweets_data_path = '/Users/chaitanyakalantri/Desktop/BDA/Project/testMe.txt'

tweets_file = open(tweets_data_path, "r")
for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)
    except:
        continue
        
tweets = pd.DataFrame()

tweets['text'] = list([tweet.get('text') for tweet in tweets_data])
tweets['lang'] = list([tweet.get('lang') for tweet in tweets_data])
tweets['created_at'] = list([tweet.get('created_at') for tweet in tweets_data])
tweets['country'] = list([tweet.get('place').get('country') if tweet.get('place') != None else None for tweet in tweets_data])

tweets.to_csv("text3.csv")