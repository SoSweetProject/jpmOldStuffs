from __future__ import division
import logging
import argparse
import pymongo
import gridfs
import ujson
from collections import defaultdict

logger = logging.getLogger(__name__)
logger_debug_frequency = 10000

def parseArgs():
    parser = argparse.ArgumentParser(description='Compute basic statistics for each community.')
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database. Host is assumed to be localhost and port 27017.')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args()


def languageDistribution(db,community):
    tweets=db.tweets.find({'community':community},{'language':True})
    user=defaultdict(int)
    twitter=defaultdict(int)
    datasift=defaultdict(float)
    for tweet in tweets:
        try:
            user[tweet['language']['user']]+=1
            twitter[tweet['language']['twitter']]+=1
            if 'datasift' in tweet['language']:
                datasift[tweet['language']['datasift']['language']]+=tweet['language']['datasift']['confidence']/100
            else:
                datasift[None]+=1
        except Exception as e:
            print tweet
            raise e
    user=[(n/sum(user.values()),l) for l,n in user.items()]
    twitter=[(n/sum(twitter.values()),l) for l,n in twitter.items()]
    datasift=[(n/sum(datasift.values()),l) for l,n in datasift.items()]
    user.sort()
    twitter.sort()
    datasift.sort()
    return user,twitter,datasift


def main():
    args = parseArgs()

    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log_destination == 'file':
        import inspect
        handler = logging.FileHandler(inspect.getfile(inspect.currentframe()).split('.')[0]+'.log')
    else:
        import sys
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)
    logger.debug(vars(args))

    uri = "mongodb://%s:%d/%s"%("localhost", 27017, args.mongoDatabase)
    db = pymongo.MongoClient(uri)[args.mongoDatabase]
    fs = gridfs.GridFS(db,collection='fs_communities_frequency_distributions')
    logger.info('connected to %s'%uri)

    for community in db.communities.find(timeout=False):
        logger.debug("treating community %d"%community['id'])
        stats={}
        stats['nUsers'] = len(community['users'])
        stats['nTweets'] = db.tweets.find({'user': {'$in':community['users']}}).count()
        fd=ujson.load(fs.get(community['frequencyDistribution']))
        stats['nWords']=len(fd)
        stats['nTokens']=sum(fd.values())
        logger.debug('updating community with %s'%str(stats))
        db.communities.update({u'_id':community[u'_id']}, {'$set':{'stats' : stats}})

if __name__ == '__main__':
    main()
