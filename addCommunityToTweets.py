import pymongo
import logging
import argparse
import sys
import inspect

logger = logging.getLogger(__name__)
logger_debug_frequency = 10000


def parseArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database. Host is assumed to be localhost, port 27017 and collection "users".')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args(), parser

def main():
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

    uri = "mongodb://%s:%d/%s"%("localhost", 27017, args.mongoDatabase)
    logger.info("Connecting to %s"%uri)
    db = pymongo.MongoClient(uri)[args.mongoDatabase]
    logger.info("Connected")

    i=0
    nCommunities=db.communities.find().count()
    for c in db.communities.find(timeout=False):
        logger.info("treating community %d (%d/%d)"%(c['id'],i,nCommunities))
        bulk = db.tweets.initialize_unordered_bulk_op()
        for user in c['users']:
            bulk.find({'user':user}).update({'$set':{'community':c['id']}})
        result = bulk.execute()
        logger.info(str(result))
        i+=1

if __name__ == '__main__':
    main()
