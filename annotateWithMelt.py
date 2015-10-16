#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals   #pour l'encodage
import meltWrapper
import pymongo 
import logging 
import inspect
import datetime
import time
import multiprocessing
import Queue
import os
import argparse
import sys


def parseArgs():
    parser = argparse.ArgumentParser(description="add community id to users and tweets")
    parser.add_argument('--mongoServerHost', default='cornichon.lip.ens-lyon.fr', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
    parser.add_argument('--mongoServerUsername', default="SoSweetWriter", type=str, help='Mongo server username')
    parser.add_argument('--mongoServerPassword', default="SSW2015", type=str, help='Mongo server password')
    parser.add_argument('--mongoDatabase', '-m', required=False, default='SoSweet', help='name of the mongo database. Host is assumed to be localhost, port 27017 and collection "users". You must either provide a sqlite database, a json file name, or a mongoDB database')
    parser.add_argument('--bulkSize', '-b', type=int, required=False, default=500, help='size of bulks of tweets')
    parser.add_argument('--sleepTime', '-s', type=int, required=False, default=60, help='number of seconds main process sleeps')
    parser.add_argument('--nWorkers', '-w', type=int, required=False, default=100, help='number of workers')
    parser.add_argument('--dontUpdateDB', action='store_true', help='specify whether the DB should actualy by updated. Turn false for debuging purpose')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args(), parser

logger = logging.getLogger(__name__)

def pretty_time_delta(seconds):
    sign_string = '-' if seconds < 0 else ''
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%s%dd%dh%dm%ds' % (sign_string, days, hours, minutes, seconds)
    elif hours > 0:
        return '%s%dh%dm%ds' % (sign_string, hours, minutes, seconds)
    elif minutes > 0:
        return '%s%dm%ds' % (sign_string, minutes, seconds)
    else:
        return '%s%ds' % (sign_string, seconds)

def tagging(queueIn,queueOut):
    logger.info("%d has started"%(os.getpid()))
    while(True):
        logger.info("%d waits for tweets from queue"%(os.getpid()))
        rawTexts, ids = queueIn.get()
        logger.debug("%d has got %d tweets from queue"%(os.getpid(),len(rawTexts)))

        MElt_bin="MElt"
        MElt_options="-TNKP"
        melt = meltWrapper.meltWrapper(MElt_bin, MElt_options)

        logger.info("%d starts tagging %d tweets"%(os.getpid(),len(rawTexts)))
        start = datetime.datetime.now()
        textes_tagged = melt.tagListOfTexts(rawTexts)
        stop=datetime.datetime.now()
        logger.info("%d has tagged %d tweets in %s"%(os.getpid(),len(rawTexts), pretty_time_delta((stop - start).seconds)))

        queueOut.put((textes_tagged, ids, rawTexts))
        logger.debug("%d has queued results"%(os.getpid()))
        logger.debug("Queue has %d elements"%(queueOut.qsize()))


def main():
    print "print test"
    args, parser = parseArgs()
    
    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log_destination == 'file':
        handler = logging.FileHandler(inspect.getfile(inspect.currentframe()).split('.')[0]+'.log')
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    if args.mongoServerUsername and args.mongoServerPassword:
        uri = "mongodb://%s:%s@%s:%d/%s"%(args.mongoServerUsername, args.mongoServerPassword, args.mongoServerHost, args.mongoServerPort, args.mongoDatabase)
    else:
        uri = "mongodb://%s:%d/%s"%(args.mongoServerHost, args.mongoServerPort, args.mongoDatabase)
    logger.info("Connecting to %s"%uri)
    db = pymongo.MongoClient(uri)[args.mongoDatabase]
    logger.info("Connected")
    
    
    queueUntagged = multiprocessing.Queue()
    queueTagged = multiprocessing.Queue()
    workers=[]
    logger.info("starting workers")
    for i in range(args.nWorkers):
        p = multiprocessing.Process(target=tagging, args=(queueUntagged,queueTagged))
        workers.append(p)
        p.start()
        
    logger.info("starting main loop")
    veryStart = datetime.datetime.now()
    nTweetsTreated=0 
    
    while True:
        while (queueUntagged.qsize()>=args.nWorkers and queueTagged.qsize()==0):
            logger.info("queueUntagged is full, queueTagged is empty, main process sleeps")
            time.sleep(args.sleepTime)

        if queueUntagged.qsize()<args.nWorkers*2:
            listeDeTextes=[]
            listeDeId=[]
            logger.debug('main process fill list of tweets')
            tweets = db.tweets.find({'melt':{'$exists':False}},{'tweet':1,'id':1}).limit(args.bulkSize)
            for tweet in tweets:
                pretreatedTweet=tweet['tweet'].replace("\n"," ").replace("\r"," ").replace('\x85',' ').replace('\xad','').strip()
                if len(pretreatedTweet)==0:
                    db.tweets.update({'id':tweet['id']}, {'$set':{'melt':[]}})
                else:    
                    listeDeTextes.append(pretreatedTweet)
                    listeDeId.append(tweet['id'])        
            bulkUpdate = db.tweets.initialize_unordered_bulk_op()
            for id in listeDeId: 
                bulkUpdate.find({'id':id}).update({'$set':{'melt':None}})#We set melt to none to be sure these tweets will not be selected again while they're being tagged
            result = bulkUpdate.execute()
            
            logger.debug("main process put %d tweets in queueUntagged"%len(listeDeTextes))
            queueUntagged.put((listeDeTextes,listeDeId))
        logger.debug('main process checks queueTagged (has %d elements)'%queueTagged.qsize())
        
        if queueTagged.qsize()>0:
            (tagged_texts, ids, rawTexts) = queueTagged.get()
            if len(tagged_texts) != len(rawTexts):
                logger.critical('Incorrect number of tagged texts! %d expected, %d found!'%(args.bulkSize,len(tagged_texts)))
                try:
                    for t in rawTexts:
                        print t.encode("utf-8")
                except (UnicodeDecodeError, UnicodeEncodeError):
                    try:
                        print type(t)
                        print t
                    except (UnicodeDecodeError, UnicodeEncodeError):
                        logger.critical("can't write this tweet: %s"%t.encode("utf-8"))
                        sys.exit('tried my best')
                for id in ids:
                    print id
                try:
                    for t in tagged_texts:
                        print t.encode("utf-8")
                except (UnicodeDecodeError, UnicodeEncodeError):
                    try:
                        print type(t)
                        print t
                    except (UnicodeDecodeError, UnicodeEncodeError):
                        logger.critical("can't write this tweet: %s"%t.encode("utf-8"))
                        sys.exit('tried my best')
                sys.exit('Incorrect number of tagged texts! %d expected, %d found!'%(args.bulkSize,len(tagged_texts)))
            logger.info('main process has found %d tagged tweets, building bulk update'%len(tagged_texts))
            if args.dontUpdateDB:
                logger.warning('dontUpdateDB option is set. Not updating database')
            else:
                bulkUpdate = db.tweets.initialize_unordered_bulk_op()
                for i in range(len(ids)): 
                    bulkUpdate.find({'id':ids[i]}).update({'$set':{'melt':tagged_texts[i]}})
                logger.info('bulk updating mongodb')
                start = datetime.datetime.now()
                result = bulkUpdate.execute()
                stop = datetime.datetime.now()    
                logger.info("%d tweets updated in mongo in %s"%(len(tagged_texts), pretty_time_delta((stop - start).seconds)))                
            nTweetsTreated+=len(tagged_texts)
            logger.info("%d tweets treated in %s"%(nTweetsTreated, pretty_time_delta((stop - veryStart).seconds)))
        



if __name__ == '__main__':
    main()








    
    
