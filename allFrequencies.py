import logging
import argparse
import pymongo
import gridfs
import ujson
from collections import Counter

logger = logging.getLogger(__name__)
logger_debug_frequency = 10000

def parseArgs():
    parser = argparse.ArgumentParser(description='Compute the global frequencies.')
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database. Host is assumed to be localhost and port 27017.')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args()


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

    logger.info('computing frequencies.')
    frequencies = Counter()
    delayed = [177,334,379,400, 574]
    for community in db.communities.find(timeout=False):
        logger.debug("treating community %d"%community['id'])
        if community['id'] in delayed:
            logger.debug("delaying")
            continue
        fd=Counter(ujson.load(fs.get(community['frequencyDistribution'])))
        frequencies+=fd
    logger.debug("treating delayed communities")
    for community in db.communities.find({'id':{'$in':delayed}},timeout=False):
        logger.debug("treating community %d"%community['id'])
        fd=Counter(ujson.load(fs.get(community['frequencyDistribution'])))
        frequencies+=fd
    frequencies=[{'word':i[0], 'count':i[1]} for i in frequencies.items()]

    logger.info('inserting frequencies.')
    bulkSize = 25000
    for i in range(0, len(frequencies), bulkSize):
        logger.debug("inserting words #%d to #%d"%(i, i+bulkSize-1))
        db.frequencies.insert(frequencies[i:i+bulkSize])
    logger.info('done.')

if __name__ == '__main__':
    main()
