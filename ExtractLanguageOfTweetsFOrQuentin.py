import pymongo
import logging,inspect

logger = logging.getLogger(__name__)
handler = logging.FileHandler(inspect.getfile(inspect.currentframe()).split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)


client=pymongo.MongoClient("mongodb://%s:%s@cornichon.lip.ens-lyon.fr:27017/SoSweet"%("SoSweetReader","SSR2015"))

tweets=client.SoSweet.tweets.find({},{'language.ldig':True, 'id':True, 'user':True},no_cursor_timeout=True)
f=open('/Users/jmague/temp/temp.txt','w')
f.write("tweet,user,language\n")
for i,tweet in enumerate(tweets):
    f.write("%s,%d,%s\n"%(tweet['id'], tweet['user'], tweet['language']['ldig']['lang']))
    if i%10000==0:
        logger.info("%d tweets writen"%i)
f.close()
logger.info('done.')
