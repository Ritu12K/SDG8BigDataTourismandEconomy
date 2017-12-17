
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from xlrd import open_workbook
import json
import pandas as pd

def columnExtract(fileName):
    book = open_workbook(fileName)
    xl_sheet = book.sheet_by_index(0) #If your data is on sheet 1

    raw_data = list()
    for row in range(1, xl_sheet.nrows):
        raw_data.append(xl_sheet.row(row))

    author_list = list()
    for raw in raw_data:
        author_list.append(list())
        for r in raw:
            author_list[-1].append(r.value)
    #print(author_list)
    column1 = []
    for i in author_list:
        if len(i[0])>0:
            column1.append(i[0])   
    return column1    

#Chaitanya
#Variables that contains the user credentials to access Twitter API

access_token = "1118554508-7bOIDie6oPxe3phLyliSLSEri6OOHAQYfC7YWaK"
access_token_secret = "dJE4kJVrw2bxNINAnFGzpkLDenSRUvEPewl4MTtCvZEVt"
consumer_key = "POuHVuQlzZEfzSwEQPazQ6VdU"
consumer_secret = "XvRig2kYjsWZqzTGOqoN2bUNm0uee9lhnSuN8EzGAcnBqXC9L8"



#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print (data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':


    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    
    fileName = "California"
    
    column1 = []
    column1 = columnExtract(fileName + '.xlsx')    
    
    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    #stream.filter(track=['economy', 'tourism'])
    stream.filter(track=column1)