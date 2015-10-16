import pymongo
import logging
import argparse
import sys
import inspect

logger = logging.getLogger(__name__)
logger_debug_frequency = 10000


def parseArgs():
    parser = argparse.ArgumentParser(description="add community id to users and tweets")
    parser.add_argument('--mongoServerHost', default='cornichon.lip.ens-lyon.fr', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
    parser.add_argument('--mongoServerUsername', default="SoSweetWriter", type=str, help='Mongo server username')
    parser.add_argument('--mongoServerPassword', default="SSW", type=str, help='Mongo server password')
    parser.add_argument('--mongoDatabase', '-m', required=False, default='SoSweet', help='name of the mongo database. Host is assumed to be localhost, port 27017 and collection "users". You must either provide a sqlite database, a json file name, or a mongoDB database')
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

    if args.mongoServerUsername and args.mongoServerPassword:
        uri = "mongodb://%s:%s@%s:%d/%s"%(args.mongoServerUsername, args.mongoServerPassword, args.mongoServerHost, args.mongoServerPort, args.mongoDatabase)
    else:
        uri = "mongodb://%s:%d/%s"%(args.mongoServerHost, args.mongoServerPort, args.mongoDatabase)
    logger.info("Connecting to %s"%uri)
    db = pymongo.MongoClient(uri)[args.mongoDatabase]
    logger.info("Connected")

    i=0
    nCommunities=db.communities.find().count()
    for c in db.communities.find(timeout=False):
        logger.info("treating community %d (%d/%d). %d users"%(c['id'],i,nCommunities,len(c['users'])))
        bulkForUsers = db.users.initialize_unordered_bulk_op()
        bulkForTweets = db.tweets.initialize_unordered_bulk_op()
        for user in c['users']:
            bulkForUsers.find({'id':user}).update({'$set':{'community':c['id']}})
            bulkForTweets.find({'user':user}).update({'$set':{'community':c['id']}})
        result = bulkForUsers.execute()
        logger.info(str(result))
        result = bulkForTweets.execute()
        logger.info(str(result))
        i+=1

if __name__ == '__main__':
    main()
