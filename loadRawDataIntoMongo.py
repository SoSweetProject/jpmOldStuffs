import argparse
import logging
import sqlite3
import ujson
import inspect
import sys
import pymongo
import tarfile
import tempfile

logger = logging.getLogger(__name__)
logger_debug_frequency = 10000

def parseArgs():
    parser = argparse.ArgumentParser(description='Build a network of followers and store it in a file suitable for the louvain binary and named <output_directory>/<network_name>.txt. A second file is created and give the mapping between teh nodes ID in teh network file and the ID of the user they reprensent: <output_directory>/<network_name>.txt')
    parser.add_argument('--sqliteDatabase', '-d', required=False, help='path to the database file. You must either provide a sqlite database, a json file name, or a mongoDB database')
    parser.add_argument('--input-file', '-i', required=False, help='path to the input json file. You must either provide a sqlite database, a json file name, or a mongoDB database')
    parser.add_argument('--mongoServerHost', default='localhost', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
    parser.add_argument('--mongoServerUsername', type=str, help='Mongo server username')
    parser.add_argument('--mongoServerPassword', type=str, help='Mongo server password')
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database')
    parser.add_argument('--mongoCollectionToFill', '-c', required=True, help='name of the mongo collection to create')
    parser.add_argument('--mongoCollectionToReadUsersFriendsFrom', '-C', required=True, help='name of the mongo collection to get users friends from')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args(), parser



def getUsersFromSqliteDatabase(database, nUsers=None):
    logger.info("Opening database %s" % database)
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    logger.info('fetching users')
    cursor.execute('SELECT id,friends FROM users where friends is not NULL')
    if nUsers:
        logger.warning("fetch only the %d first users"%nUsers)
        users = cursor.fetchmany(nUsers)
    else:
        users = cursor.fetchall()
    logger.info('%d users fetched'%len(users))
    return users


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
    logger.info("Connected to %s"%uri)

    outputCollection=db[args.mongoCollectionToFill]
    inputCollection = db[args.mongoCollectionToReadUsersFriendsFrom]

    outputCollection.ensure_index("id")

    logger.info("Opening %s"%args.input_file)
    tgzFile = tarfile.open(args.input_file)
    files = [f for f in tgzFile.getmembers() if f.name[-4:]=='data']
    logger.info("%s opened, %s files"%(args.input_file, len(files)))
    for i,file in enumerate(files):
        logger.info("extracting file %d/%d: %s"%(i+1, len(files), file.name))
        f=tgzFile.extractfile(file)
        logger.info("treatingting file %d/%d: %s"%(i+1, len(files), file.name))
        for line in f:
            t=ujson.loads(line)
            if outputCollection.find({'id':t['twitter']['user']['id']}).count()==0:
                logger.debug("inserting user %d"%t['twitter']['user']['id'])
                friends = inputCollection.find_one({'id':t['twitter']['user']['id']})['friends']
                outputCollection.insert({'id' : t['twitter']['user']['id'],
                                         'location' : t['twitter']['user']['location'] if 'location' in t['twitter']['user'] else "",
                                         'time_zone': t['twitter']['user']['time_zone'],
                                         'geo':[],
                                         'friends':friends}
                                         )
            if "geo" in t['twitter']:
                logger.debug('inserting coordinates (%s) in user %d'%(str( t['twitter']['geo']), t['twitter']['user']['id']))
                res=outputCollection.update({'id':t['twitter']['user']['id']}, {'$push' :{'geo' : t['twitter']['geo']}})
                logger.debug(res)





if __name__ == '__main__':
    main()
