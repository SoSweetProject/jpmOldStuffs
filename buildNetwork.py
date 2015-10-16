import argparse
import logging
import datetime
import sqlite3
import ujson
import inspect
import sys
import pymongo

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
    parser.add_argument('--mongoDatabase', '-m', required=False, help='name of the mongo database. Host is assumed to be localhost, port 27017 and collection "users". You must either provide a sqlite database, a json file name, or a mongoDB database')
    parser.add_argument('--output-dir', '-o', required=True, help='path to the output directory')
    parser.add_argument('--network-name', '-n', required=True, help='name of network.')
    parser.add_argument('--no-reciprocation', dest='reciprocation', action='store_false', help='filter out links that are not bidirectional')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args(), parser

def getUsersFromFile(fileName, nUsers=None):
    logger.info("Opening file %s" % fileName)
    f=open(fileName)
    logger.info('fetching users')
    users=[]
    if nUsers:
        logger.warning("fetch only the %d first users"%nUsers)
        for u in range(nUsers):
            user = ujson.loads(f.readline())
            if 'friends' in user and type(user['friends']) == list:
                friends=[friend if type(friend)==int else int(friend['$numberLong']) for friend in user['friends']]
                id = user['id'] if type(user['id'])==int else int(user['id']['$numberLong'])
                users.append({'id':'id', 'friends':ujson.dumps(friends)})
    else:
        for l in f:
            user = ujson.loads(l)
            if 'friends' in user and type(user['friends']) == list:
                friends=[friend if type(friend)==int else int(friend['$numberLong']) for friend in user['friends']]
                id = user['id'] if type(user['id'])==int else int(user['id']['$numberLong'])
                users.append({'id':id, 'friends':ujson.dumps(friends)})
    logger.info('%d users fetched'%len(users))
    return users

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


def getUsersFromMongoDB(username=None, password=None, host='localhost', port=27017, database=None, nUsers=None):
    if username and password:
        uri = "mongodb://%s:%s@%s:%d/%s"%(username, password, host, port, database)
    else:
        uri = "mongodb://%s:%d/%s"%(host, port, database)
    logger.info("Connecting to %s"%uri)
    db = pymongo.MongoClient(uri)[database]
    logger.info("Connected to%s"%uri)
    if nUsers:
        logger.warning("fetch only the %d first users"%nUsers)
        users = db.users.find(timeout=False).limit(nUsers)
    else:
        users = db.users.find(timeout=False)
    logger.info('%d users fetched'%users.count())
    users.rewind()
    return users

def buildNetwork(users):
    logger.info("building network")
    logger.debug("building set of ids")
    knownUsers=set([user['id'] for user in users])
    if type(users)==pymongo.cursor.Cursor:
        users.rewind()
    net = dict()
    t=datetime.datetime.now()
    logger.debug("treating users")
    if type(users)==pymongo.cursor.Cursor:
        nUsers = users.count()
        users.rewind()
    else:
        nUsers = len(users)
    for i,user in enumerate(users):
        if i%logger_debug_frequency == 0:
            logger.debug("%d/%d (%s)"%(i, nUsers, str(datetime.datetime.now()-t)))
            t=datetime.datetime.now()
        try:
            id = user['id']
            friends = user['friends'] if type(user['friends']) is list else ujson.loads(user['friends'])
            filtered_friends = set([friend for friend in friends if friend in knownUsers and friend != id])
            net[id] = filtered_friends
        except Exception as e:
            logger.info("Exception in user %d : %s"%(id, user['friends']))
            logger.info(e)
            net[id] = set()
    return net

def reciprocateNetwork(net):
    logger.info("Reciprocating network")
    t=datetime.datetime.now()
    for i,user in enumerate(net):
        if i%logger_debug_frequency == 0:
            logger.debug("%d/%d (%s)"%(i,len(net), str(datetime.datetime.now()-t)))
            t=datetime.datetime.now()
        if net[user]:
            net[user] = [friend for friend in net[user] if user in net[friend]]
            for friend in net[user]:
                net[friend].remove(user)
    return net

def writeNetwork(network, output_dir, network_name):
    networkFileName="%s/%s.%s"%(output_dir,network_name,"txt")
    logger.info("writing network into %s"%networkFileName)
    f=open(networkFileName,'w')
    correspondances = {}
    idx = 0
    i=0
    t=datetime.datetime.now()
    for source in network:
        if i%logger_debug_frequency == 0:
            logger.debug("%d/%d (%s)"%(i,len(network), str(datetime.datetime.now()-t)))
            t=datetime.datetime.now()
        i+=1
        if len(network[source])==0:
            continue
        if source in correspondances:
            source_idx=correspondances[source]
        else:
            source_idx = idx
            correspondances[source]=idx
            idx+=1
        for target in network[source]:
            if target in correspondances:
                target_idx=correspondances[target]
            else:
                target_idx = idx
                correspondances[target]=idx
                idx+=1
            f.write("%d %d\n"%(source_idx,target_idx))
    f.close()
    correspondancesFileName="%s/%s.%s"%(output_dir,network_name,"correspondances")
    f=open(correspondancesFileName,'w')
    logger.info("writing correspondances into %s"%correspondancesFileName)
    i=0
    t=datetime.datetime.now()
    for node in correspondances:
        f.write("%d %d\n"%(node, correspondances[node]))
        if i%logger_debug_frequency == 0:
            logger.debug("%d/%d (%s)"%(i,len(correspondances), str(datetime.datetime.now()-t)))
            t=datetime.datetime.now()
        i+=1
    f.close()
    return (correspondances, networkFileName, correspondancesFileName)


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

    if (args.sqliteDatabase):
        users = getUsersFromSqliteDatabase(args.sqliteDatabase)
    elif (args.input_file):
        users = getUsersFromFile(args.input_file)
    elif (args.mongoDatabase):
        users = getUsersFromMongoDB(args.mongoServerUsername, args.mongoServerPassword,args.mongoServerHost,args.mongoServerPort, args.mongoDatabase)
    else :
        parser.print_help()
        sys.exit(-1)
    network = buildNetwork(users)
    if args.reciprocation:
        network=reciprocateNetwork(network)
    (correspondances, networkFileName, correspondancesFileName)=writeNetwork(network, args.output_dir, args.network_name)
    if args.mongoDatabase:
        if args.mongoServerUsername and args.mongoServerPassword:
            uri = "mongodb://%s:%s@%s:%d/%s"%(args.mongoServerUsername, args.mongoServerPassword, args.mongoServerHost,args.mongoServerPort, args.mongoDatabase)
        else:
            uri = "mongodb://%s:%d/%s"%(args.mongoServerHost,args.mongoServerPort, args.mongoDatabase)
        db = pymongo.MongoClient(uri)["database"]
        db['network'].insert({'files' : {'network' : networkFileName,'correspondances':correspondancesFileName}})
    logger.info("done.")

if __name__ == '__main__':
    main()
