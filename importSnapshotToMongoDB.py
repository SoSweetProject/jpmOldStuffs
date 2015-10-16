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
    parser = argparse.ArgumentParser(description='Import json file onto mongoDB')
    parser.add_argument('--snapshot-dir', '-s', required=True, help='path to the data files')
    parser.add_argument('--database', '-d', help='Name of te database. Build after the directory name otherwise.')
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

    if args.snapshot_dir[-1] != '/':
        args.snapshot_dir+='/'

    if args.database:
        database = args.database
    else:
        database = "snapshot_"+args.snapshot_dir.split('/')[-2]
    uri = "mongodb://%s:%d/%s"%(args.mongoServerHost, args.mongoServerPort, database)
    logger.info("Connecting to %s"%uri)
    client = pymongo.MongoClient(uri)[database]
    logger.info("Connected to%s"%uri)


    files = glob.glob(args.snapshot_dir+'*.data')
    for file in files:
        logger.info("reading %s"%file)
        tweets = [date_hook(ujson.loads(l)) for l in open(file)]
        logger.info("%d tweets read from %s"%(len(tweets),file))
        if len(tweets)>0:
            if not args.skip_tokenization:
                logger.info("Tokenizing tweets")
                tokenizer = Tokenizer(preserve_case=True)
                tokenized_tweets = [tokenizer.tokenize(tweet['tweet']) for tweet in tweets]
                logger.info("Tagging tweets")
                tagger = TreeTagger(path_to_bin=args.path_to_treetagger, path_to_param=args.path_to_treetagger_param_file)
                tagged_tweets = tagger.tag(tokenized_tweets)
                for i in range(len(tweets)):
                    tweets[i]['tagged_tweet'] = tagged_tweets[i]
            logger.info("Loading tweets into database")
            client['tweets'].insert(tweets)

    logger.info("Loading users from %susers.db"%args.snapshot_dir)
    connection = sqlite3.connect("%susers.db"%args.snapshot_dir)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    logger.info('fetching users')
    cursor.execute('SELECT id,friends FROM users where friends is not NULL')
    users = cursor.fetchall()
    logger.info('%d users fetched'%len(users))
    bulk_size=25000
    nUsersInserted=0
    usersToBeInserted=[]
    for user in users:
        id = user['id']
        friends = ujson.loads(user['friends'])
        usersToBeInserted.append({'id':id, 'friends':friends})
        if len(usersToBeInserted)>=bulk_size:
            client['users'].insert(usersToBeInserted)
            usersToBeInserted=[]
            nUsersInserted+=bulk_size
            logger.info("%d users insered"%nUsersInserted)
    client['users'].insert(usersToBeInserted)
    logger.info("all users insered.")

    logger.info("done.")



if __name__ == "__main__":
    main()
