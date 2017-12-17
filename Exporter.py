# -*- coding: utf-8 -*-
import sys,getopt,datetime,codecs
from xlrd import open_workbook
if sys.version_info[0] < 3:
    import got
else:
    import got3 as got


def columnExtract():

    fileName = "TouristPlaces.xlsx"
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
            	#print i[0]
            	#i[0] = i[0].lstrip('u')[1:-1]
                column1.append(i[0])
        #break;        
    #print ', '.join(repr(x.encode('ascii')) for x in column1)   
    #return ' or '.join(repr(x).lstrip('u')[1:-1] for x in column1)
    column1 = [ ''.join(x.split()).lower() for x in column1]
    #print col
    return column1


def main():

	
	outputFileName = "August.csv"
	outputFile = codecs.open(outputFileName, "w+", "utf-8")
	try:
	
		#opts, args = getopt.getopt(argv, "", ("username=", "near=", "within=", "since=", "until=", "querysearch=", "toptweets", "maxtweets=", "output="))
		arg = "check"
		tweetCriteria = got.manager.TweetCriteria()
		outputFileName = "August.csv"

		searchcols = columnExtract()
		#printsearchtext
		#tweetCriteria.querySearch = "barackobama or triump"
		tweetCriteria.since       = "2016-08-01"
		tweetCriteria.until       = "2016-08-30"
		#tweetCriteria.maxTweets   = 1000
		
				
		outputFile = codecs.open(outputFileName, "w+", "utf-8")

		outputFile.write('date;favorites;text;geo;mentions;hashtags;id')

		print('Searching...\n')
		print len(searchcols)
		
		searchcols = [x for x in searchcols if x not in ["name","genre","county","rankingbytripadvisor","starratingbytripadvisor"]]
		print len(searchcols)
		for i in range(0,len(searchcols),5):
				
			col = ' OR '.join(repr(x).lstrip('u')[1:-1] for x in searchcols[i:i+5])
			#print col
			tweetCriteria.querySearch = col
		
			#print tweetCriteria

			#print(tweetCriteria)
			def receiveBuffer(tweets):
				for t in tweets:
					#print t
					outputFile.write(('\n%s;"%s"' % (t.date.strftime("%Y-%m-%d %H:%M"), t.text)))
				outputFile.flush();
				print('More %d saved on file...\n' % len(tweets))

			got.manager.TweetManager.getTweets(tweetCriteria, receiveBuffer)

	except arg:
		print('Arguments parser error, try -h' + arg)
	finally:
		outputFile.close()
		print('Done. Output file generated "%s".' % outputFileName)

if __name__ == '__main__':
	main()
