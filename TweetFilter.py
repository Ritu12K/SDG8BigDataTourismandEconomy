
from xlrd import open_workbook
import json
from fuzzywuzzy import fuzz

tweetdict = {}
i = 0

columns = []

def list_files(dir):                                                                                                  
    r = []                                                                                                            
    subdirs = [x[0] for x in os.walk(dir)]                                                                            
    for subdir in subdirs:                                                                                            
        files = os.walk(subdir).next()[2]                                                                             
        if (len(files) > 0):                                                                                          
            for file in files:                                                                                        
                r.append(subdir + "/" + file)                                                                         
    return r   

def function(file):
    outpath = file[:-4]+"out.txt"

    #print file,outpath
    #outputfile = open(outpath,'w')
    
    #print file
    archive_path = file
    outfile_path1 = file[:-4]

    global i
    i = 0
    
    with open(archive_path, 'rb') as source, open(outfile_path1, 'wb') as dest:
        dest.write(bz2.decompress(source.read()))


    #print outfile_path1
    g = open(outfile_path1, 'r')

    for l in g:
        #print l
        res = filterline(json.loads(l.decode('utf-8')))
        if res is None:
            continue
        else:
            if len(res) != 0:
                result.append(res)
    #print result

    #outputfile.write(result)

    #print outpath
    '''
    for item in result:
        outputfile.write(json.dumps(item))
    #print len(result)'''
    #outputfile.close()

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


def filterline(line):
    #print line
    if not line:
        return None
    lang = "lang"
    user = "user"
    
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
                global i
                i+=1    
                tweetdict[i] = {"cr_at":line['created_at'],"tags":tags}

              
                return 
                #return line
             

        
        score = [ fuzz.token_set_ratio(x,line['text']) for x in columns]
        
        #print max(score)
        if max(score) > 55:
            #print fuzz.token_sort_ratio(x,line['text'])
            if(line['text'] is None):
            	print "=============\n"
            	return;
            newtweet['text'] = line['text']
            newtweet['created_at'] = line['created_at']

            tags = []
            #print tags
            for hashtag in hashtags:
                tags.append(hashtag["text"].lower().encode('ascii','ignore'))     
            global i
            i+=1    
            #print "yeah\n"
            if len(tags) != 0:
                res = []
                res.append(columns[score.index(max(score))])   
                tweetdict[i] = {"cr_at":line['created_at'],"tags": res }   
            
            return
            

    return None   


if __name__ == '__main__':

    fileName = "TouristPlaces.xlsx"

    columns = columnExtract(fileName)

    columns = [ ''.join(x.split()).lower() for x in columns]

        
    #print columns

    #columns = ["onefinedayinbkk","onefinedayinbkk","besties"]

    

    result = []
    path_to_folder = '/Users/ritukaushik/Downloads/BDAdata'

    import os
    import bz2
    files = []

    #path_to_folder = '/Users/ritukaushik/Downloads/BDAdata/02'
    r = list_files(path_to_folder)
    print r
    for it in r:
        it = str(it)
        if it.endswith('.bz2'):
            files.append(it)
    global i
    for file in files:
        function(file)
        print file
        print i

    print len(tweetdict)
    print len(files)
    with open(path_to_folder+"/outpath"+str(i)+".txt","w") as f:
        json.dump(tweetdict,f)    
    
    
                  
