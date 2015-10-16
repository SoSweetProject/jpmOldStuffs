from __future__ import division
import argparse
import logging
import ujson
import nltk.probability
import pymongo
import gridfs
import sys
import numpy as np


logger = logging.getLogger(__name__)
logger_debug_frequency = 10000

def parseArgs():
    parser = argparse.ArgumentParser(description='Build the frequency distribution of each community.')
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database. Host is assumed to be localhost, port 27017 and collection "users". You must either provide a sqlite database, a json file name, or a mongoDB database')
    parser.add_argument('--frenchOnly', '-f', action='store_true', help='Specify wether only tweets tagged in French should be treated.')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args()

# @author: jonathanfriedman - http://stats.stackexchange.com/questions/29578/jensen-shannon-divergence-calculation-for-3-prob-distributions-is-this-ok
def jensen_shannon_divergence(x,y):
    # Jensen-shannon divergence
    x = np.array(x)
    y = np.array(y)
    x[np.isnan(x)] = 0
    y[np.isnan(y)] = 0
    x/=x.sum()
    y/=y.sum()
    d1 = x*np.log2(2*x/(x+y))
    d2 = y*np.log2(2*y/(x+y))
    d1[np.isnan(d1)] = 0
    d2[np.isnan(d2)] = 0
    d = 0.5*np.sum(d1+d2)
    return d

def isNotHashtag(w):
    return w[0]!='#' if len(w)>0 else True

def isNotMention(w):
    return w[0]!='@' if len(w)>0 else True

def isNotURL(w):
    return w[:7]!='http://' if len(w)>7 else True

def isNotPunctuation(w):
    return w[0] not in ['?','.',',','!','(',')','"',"'",'$','&','%'] if len(w)==0 else True


def filter_frequency_distribution(fd,frequency=0, filters=[]):
    """
    fd: the frequency distribution to filter
    frequency: the minimal frequency
    filter: a list f_i of function. Only the words w verifying all f_i(w) are kept"""
    result = {w:f for w,f in fd.items() if f>=frequency}
    for filter in filters:
        result = {w:f for w,f in result.items() if filter(w)}
    return result

def main():
    args = parseArgs()
    logger.debug(args)

    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log_destination == 'file':
        import inspect
        handler = logging.FileHandler(inspect.getfile(inspect.currentframe()).split('.')[0]+'.log')
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    logger.info("computing frequency distribution of communities")

    logger.info("finding communities")
    uri = "mongodb://%s:%d/%s"%("localhost", 27017, args.mongoDatabase)
    db = pymongo.MongoClient(uri)[args.mongoDatabase]
    fs = gridfs.GridFS(db,collection='fs_communities_frequency_distributions')

    for community in db.communities.find({'frequencyDistribution_fr':{'$exists':False}},timeout=False):
        logger.debug("Computing frequencies for community %i"%community['id'])
        freqDistribution=nltk.probability.FreqDist()
        if args.frenchOnly:
            logger.debug('selecting tweets in French')
            tweets = db.tweets.find({'community' : community['id'], '$or': [{'language.twitter':{'$eq':'fr'}}, {'language.datasift.language':{'$eq':'fr'}}]}, {'tagged_tweet':True}, timeout=False)
        else :
            logger.debug('selecting tweets in any language')
            tweets = db.tweets.find({'community' : community['id']}, {'tagged_tweet':True}, timeout=False)
        for tweet in tweets:
            freqDistribution.update([token[0] for token in tweet['tagged_tweet']])
        fs_id = fs.put(ujson.dumps(freqDistribution))
        if args.frenchOnly:
            logger.debug('updating community with frequencyDistribution_fr')
            db.communities.update({'id':community['id']},{'$set':{'frequencyDistribution_fr':fs_id}})
        else:
            logger.debug('updating community with frequencyDistribution')
            db.communities.update({'id':community['id']},{'$set':{'frequencyDistribution':fs_id}})

    logger.info("done.")

if __name__ == '__main__':
    main()
