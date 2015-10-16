import pymongo
import logging
import argparse
import sys
import inspect
import networkx as nx


logger = logging.getLogger(__name__)
logger_debug_frequency = 10000


def parseArgs():
    parser = argparse.ArgumentParser(description='Build a dictionary, where keys are users and value the GPS coordinates of their tweet.')
    parser.add_argument('--mongoServerHost', default='localhost', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
    parser.add_argument('--mongoServerUsername', type=str, help='Mongo server username')
    parser.add_argument('--mongoServerPassword', type=str, help='Mongo server password')
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database. Host is assumed to be localhost, port 27017.')
    parser.add_argument('--output-file', '-o', required=True, help='name of the output file')
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
        uri = "mongodb://%s:%s@%s:%d/%s"%(args.mongoServerUsername, args.mongoServerPassword, args.mongoServerHost,args.mongoServerPort, args.mongoDatabase)
    else:
        uri = "mongodb://%s:%d/%s"%(args.mongoServerHost,args.mongoServerPort, args.mongoDatabase)
    logger.info("Connecting to %s"%uri)
    db = pymongo.MongoClient(uri)[args.mongoDatabase]
    logger.info("Connected")

    net = nx.DiGraph()
    for c in db.communities.find():
        net.add_node(c['id'], nSpeakers=len(c['users']))
    correspondances = {int(l.split(' ')[1]): int(l.split(' ')[0]) for l in open(db.network.find_one({'files':{'$exists':True}})['files']['correspondances'])}
    logger.debug("loading users")
    users = {u['id'] : u['community'] for u in db.users.find({'community':{'$exists':True}},{'id':True, 'community':True})}
    logger.debug("reading file")
    logger.debug("counting lines")
    num_lines = sum(1 for line in open(db.network.find_one({'files':{'$exists':True}})['files']['network']))
    logger.debug("%d lines"%num_lines)
    i=0
    for l in open(db.network.find_one({'files':{'$exists':True}})['files']['network']):
        u0,u1=l.split(" ")
        n0=users[correspondances[int(u0)]]
        n1=users[correspondances[int(u1)]]
        if net.has_edge(n0,n1):
            net[n0][n1]['weight']+=1
        else :
            net.add_edge(n0,n1,weight=1)
        if i%logger_debug_frequency==0:
            logger.debug("%d/%d lines read"%(i,num_lines))
        i+=1
    logger.info("writing file: %s"%args.output_file)
    nx.write_gexf(net,args.output_file)
    logger.info("done.")

if __name__ == '__main__':
    main()
