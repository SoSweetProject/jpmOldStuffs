import sys
import ujson
import pymongo
import logging
import argparse
import sqlite3


logger = logging.getLogger(__name__)
logger_debug_frequency = 10000


def parseArgs():
    parser = argparse.ArgumentParser(description='Import json file onto mongoDB')
    parser.add_argument('--snapshot-dir', '-s', required=True, help='path to the data files')
    parser.add_argument('--database', '-d', help='Name of te database. Build after the directory name otherwise.')
    parser.add_argument('--mongoServerHost', default='localhost', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
    parser.add_argument('--mongoServerUsername', type=str, help='Mongo server username')
    parser.add_argument('--mongoServerPassword', type=str, help='Mongo server password')
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
        handler = logging.FileHandler('importUsersFromSnapshotToMongoDB.log')
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
    if args.mongoServerUsername and args.mongoServerPassword:
        uri = "mongodb://%s:%s@%s:%d/%s"%(args.mongoServerUsername, args.mongoServerPassword, args.mongoServerHost, args.mongoServerPort, database)
    else:
        uri = "mongodb://%s:%d/%s"%(args.mongoServerHost, args.mongoServerPort, database)
    logger.info("Connecting to %s"%uri)
    client = pymongo.MongoClient(uri)[database]
    logger.info("Connected to %s"%uri)


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
