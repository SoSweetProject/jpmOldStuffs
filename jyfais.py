# -*- coding: utf-8 -*-
import pymongo
import logging
import inspect
import codecs
import sys

logger = logging.getLogger(__name__)
handler = logging.FileHandler(inspect.getfile(inspect.currentframe()).split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info("Connecting to mongodb")
db = pymongo.MongoClient("mongodb://SoSweetReader:SSR2015@cornichon.lip.ens-lyon.fr:27017/SoSweet")['SoSweet']
logger.info("Connected")




f=open('jyfais.txt','w')

for tweet in db.tweets.find({'language.ldig.lang':'fr'},timeout=False):
    if tweet['melt'] is not None:
        for i in range(len(tweet['melt'])-1):
            token=tweet['melt'][i]
            nextToken=tweet['melt'][i+1]
            if token['token']=='y' and nextToken['tag']=='V' and nextToken['token']!='vais' and nextToken['token']!='a':
                text=tweet['tweet'].encode('utf8')
                annotatedText= ' '.join(['%s_%s'%(t['token'],t['tag']) for t in tweet['melt']]).encode('utf8')
                id=tweet['id'].encode('utf8')
                #logger.info("%s\t%s"%(text, id))
                f.write("%s\t%s\n"%(text, id))
f.close()
