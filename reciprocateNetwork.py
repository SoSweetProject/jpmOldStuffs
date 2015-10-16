import argparse
import logging
import datetime
import sqlite3
import ujson
import inspect
import sys
import pymongo
from collections import defaultdict

logger = logging.getLogger(__name__)
logger_debug_frequency = 1000000

def parseArgs():
    parser = argparse.ArgumentParser(description='Reciprocate a network built with buildNetwork.py')
    parser.add_argument('--network-dir', '-d', required=True, help='path to the network directory')
    parser.add_argument('--network-name', '-n', required=True, help='name of network.')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args(), parser

def readNetwork(directory, name):
    fileName = "%s/%s.txt"%(directory, name)
    netFile=open(fileName)
    logger.info("%s opened"%fileName)
    network=defaultdict(list)
    for i,l in enumerate(netFile):
        src,dest=l.split(' ')
        network[int(src)].append(int(dest))
        if i%logger_debug_frequency==0:
            logger.debug("%d lines read"%i)
    return network

def readCorrespondances(directory, name):
    fileName = "%s/%s.correspondances"%(directory, name)
    correspFile=open(fileName)
    logger.info("%s opened"%correspFile)
    correpondances={}
    for i,l in enumerate(correspFile):
        twitterID, localID=l.split(' ')
        network[int(src)].append(int(dest))
        if i%logger_debug_frequency==0:
            logger.debug("%d lines read"%i)
    return network

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

    network=readNetwork(args.network_dir,args.network_name)
    correpondances = readCorrespondances(args.network_dir,args.network_name)
    network=buildNetwork.reciprocateNetwork(network)
    buildNetwork.writeNetwork(network, args.network_dir, args.network_name+"-reciprocated")

    logger.info("done.")

if __name__ == '__main__':
    main()
