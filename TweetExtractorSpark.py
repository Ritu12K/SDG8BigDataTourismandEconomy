
import json

from pyspark import SparkContext, SparkConf


from xlrd import open_workbook


from fuzzywuzzy import fuzz

conf = SparkConf().setAppName("SatelliteImageAnalysis")
sc = SparkContext(conf=conf)


path = "/Users/ritukaushik/Downloads/BDAdata/02/11/*.json"

textRDD = sc.textFile(path)

tweetRDD = textRDD.map(lambda x:x)

#tweetRDD.take(1)


i = 0


# In[212]:

def columnExtract(fileName):
    book = open_workbook(fileName)

    column1 = list()

    for xl_sheet in book.sheets():

        raw_data = list()
        for row in range(1, xl_sheet.nrows):
            raw_data.append(xl_sheet.row(row))

        author_list = list()
        for raw in raw_data:
            author_list.append(list())
            for r in raw:
                author_list[-1].append(r.value)
        #print(author_list)
        
        for i in author_list:
            if len(i[0])>0:
                column1.append(i[0])  
        
    return column1


# In[231]:

def filterline(line):
    #print line
    if line[-1] == '\n':
        line = line[:-1]
    #return line  
    l = json.loads(line)    
    #return l
    if not line:
        return None    
    line = l
    #return line
    
    lang = "lang"
    user = "user"
    tweetdict = {}
    if lang not in line and user not in line:
        #print "retun"
        return None
    
    newtweet = dict()       
    if line['user']['lang'] == "en":
        #print "eng"
        #if any( fuzz.token_set_ratio(x,line['text']) for x in columns):
        
        hashtags = line['entities']['hashtags']        
        if len(hashtags) > 0:
        	#print len(tags)
            tags = []
            #print tags
            for hashtag in hashtags:
                tags.append(hashtag["text"].lower().encode('ascii','ignore'))    
            #print tags
            #print columns


            if any([item in tags for item in columns]):
                #print tags
                newtweet['text'] = line["text"]
                newtweet['created_at'] = line["created_at"]
                newtweet['tags']      = tags
                #global i
                i=0    
                tweetdict[i] = {"cr_at":line['created_at'],"tags":item}

              
                return tweetdict
                #return line
             



filteredtweetRDD = tweetRDD.map(lambda x:filterline(x))

columns = columnExtract('/Users/ritukaushik/Downloads/TouristPlaces.xlsx')

columns = [ ''.join(x.split()).lower() for x in columns]

filteredtweetRDD = filteredtweetRDD.filter(lambda x: x is not None).filter(lambda x: len(x)>0)

print(filteredtweetRDD.collect())



