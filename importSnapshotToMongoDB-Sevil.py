import sys
import ujson
import pymongo
import dateutil.parser
import logging
import argparse


logger = logging.getLogger(__name__)
logger_debug_frequency = 10000


def parseArgs():
    parser = argparse.ArgumentParser(description='Import json file onto mongoDB')
    parser.add_argument('--file', '-f', required=True, help='path to the data files')
    parser.add_argument('--database', '-d', help='Name of te database. Build after the directory name otherwise.')
    parser.add_argument('--mongoServerHost', default='localhost', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
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
    logger.info("Connected to %s"%uri)


    logger.info("reading %s"%file)
    tweets = [date_hook(ujson.loads(l)) for l in open(file)]
    logger.info("%d tweets read from %s"%(len(tweets),file))
    if len(tweets)>0:
        logger.info("Loading tweets into database")
        client['tweets'].insert(tweets)
    logger.info("done.")

if __name__ == "__main__":
    main()
