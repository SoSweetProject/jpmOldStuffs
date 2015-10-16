import sys
import ujson
import pymongo
import dateutil.parser
import logging
import argparse
from happyfuntokenizing import Tokenizer
from TreeTaggerWrapper import TreeTagger
import glob
import sqlite3


logger = logging.getLogger(__name__)
logger_debug_frequency = 10000


def parseArgs():
    parser = argparse.ArgumentParser(description='Import json files onto mongoDB')
    parser.add_argument('--data-dir', '-d', required=True, help='path to the data files.')
    parser.add_argument('--database', '-D', required=True, help='Name of te database.')
    parser.add_argument('--skip-tokenization', action='store_true', help='Skip tokenization and POS tagging')
    parser.add_argument('--mongoServerHost', default='localhost', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
    parser.add_argument('--path-to-treetagger', default='/Users/jmague/Documents/work/treetagger/bin/tree-tagger')
    parser.add_argument('--path-to-treetagger-param-file', default='/Users/jmague/Documents/work/treetagger/lib/french-utf8.par')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args()


def date_hook(dct):
    if 'date' in dct:
        dct['date'] = dateutil.parser.parse(dct['date'])
    return dct


def main():
    args = parseArgs()

    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log_destination == 'file':
        handler = logging.FileHandler('importSnapshotToMongoDB.log')
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    if args.data_dir[-1] != '/':
        args.data_dir+='/'

    uri = "mongodb://%s:%d/%s"%(args.mongoServerHost, args.mongoServerPort, args.database)
    logger.info("Connecting to %s"%uri)
    client = pymongo.MongoClient(uri)[args.database]
    logger.info("Connected to%s"%uri)

    files = glob.glob(args.data_dir+'*.data')
    for file in files:
        logger.info("reading %s"%file)
        tweets = [date_hook(ujson.loads(l)) for l in open(file)]
        logger.info("%d tweets read from %s"%(len(tweets),file))
        if len(tweets)>0:
            if not args.skip_tokenization:
                logger.info("Tokenizing tweets")
                tokenizer = Tokenizer(preserve_case=True)
                tokenized_tweets = [tokenizer.tokenize(tweet['twitter']['text']) for tweet in tweets]
                logger.info("Tagging tweets")
                tagger = TreeTagger(path_to_bin=args.path_to_treetagger, path_to_param=args.path_to_treetagger_param_file)
                tagged_tweets = tagger.tag(tokenized_tweets)
                for i in range(len(tweets)):
                    tweets[i]['tagged_tweet'] = tagged_tweets[i]
            logger.info("Loading tweets into database")
            client['tweets'].insert(tweets)

    logger.info("done.")



if __name__ == "__main__":
    main()
