import argparse
import logging
import os
import collections
import sys
import inspect
import pymongo

logger = logging.getLogger(__name__)


def parseArgs():
    parser = argparse.ArgumentParser(description='Run Louvain binaries to find communities.')
    parser.add_argument('--mongoServerHost', default='localhost', help='Mongo server host')
    parser.add_argument('--mongoServerPort', default=27017, type=int, help='Mongo server port')
    parser.add_argument('--mongoServerUsername', type=str, help='Mongo server username')
    parser.add_argument('--mongoServerPassword', type=str, help='Mongo server password')
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database. Host is assumed to be localhost, port 27017 and collection "users".')
    parser.add_argument('--binaries', '-b', required=True, help='path to the louvain binaries')
    parser.add_argument('--output-file', '-o', required=True, help='path to the output file')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args(), parser


def getLevelsSize(networkFileName, louvainDirectory):
    # return a list of the size of each level of the hierarchy
    basedir = os.path.dirname(networkFileName)
    baseFileName = os.path.basename(networkFileName).split('.')[0]
    treeFileName ="%s/%s.%s" %(basedir,baseFileName,"tree")
    command = "%shierarchy %s -l -1"%(louvainDirectory, treeFileName)
    logger.debug(command)
    pipe = os.popen(command)
    levels=[]
    pipe.readline()
    for l in pipe:
        levels.append(int(l.split(': ')[1].split(' ')[0]))
    return levels

def findCommunities(networkFileName, louvainDirectory):
    basedir = os.path.dirname(networkFileName)
    baseFileName = os.path.basename(networkFileName).split('.')[0]
    binFileName ="%s/%s.%s" %(basedir,baseFileName,"bin")
    treeFileName ="%s/%s.%s" %(basedir,baseFileName,"tree")

    if louvainDirectory[-1] != '/' :
        louvainDirectory+='/'

    logger.info("converting %s to %s"%(networkFileName, binFileName))
    os.popen("%sconvert -i %s -o %s"%(louvainDirectory, networkFileName, binFileName))

    logger.info("finding community structure from %s ; writing to %s"%(binFileName, treeFileName))
    os.popen("%slouvain %s -l -1 > %s"%(louvainDirectory, binFileName, treeFileName))
    logger.info("community writen in %s"%treeFileName)
    return (binFileName, treeFileName)

def readCorrespondancesFile(correpondancesFileName):
    f=open(correpondancesFileName)
    correspondances = {}
    for l in f:
        node, index = map(int,l.split(' '))
        correspondances[index]=node
    return correspondances

def getCommunities(networkFileName, correpondancesFileName, louvainDirectory, level):
    # return a dictionary where keys are communities and values are users
    basedir = os.path.dirname(networkFileName)
    baseFileName = os.path.basename(networkFileName).split('.')[0]
    treeFileName ="%s/%s.%s" %(basedir,baseFileName,"tree")
    logger.info("reading community file: %s"%treeFileName)
    correspondances=readCorrespondancesFile(correpondancesFileName)
    command = "%shierarchy %s -l %d"%(louvainDirectory, treeFileName, level)
    pipe = os.popen(command)
    communities=collections.defaultdict(list)
    for l in pipe:
        node, community = map(int,l.split(' '))
        communities[community].append(correspondances[node])
    return communities

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
    files=db.network.find_one({'files':{'$exists':True}})['files']
    network_file = files['network']
    correpondances_file = files['correspondances']

    binFileName, treeFileName = findCommunities(network_file, args.binaries)
    db['network'].update({'files':{'$exists':True}}, {'$set':{'files.tree': treeFileName, 'files.bin':binFileName}})
    #
    levels = getLevelsSize(network_file, args.binaries)
    db['network'].insert({'communityLevels': levels})

    communities = getCommunities(network_file, correpondances_file, args.binaries, len(levels)-1)
    logger.info("%d communities"%len(communities))
    i=0
    logger.info("updating database")
    logger.debug("building users.id index")
    db['users'].ensure_index('id')
    logger.debug("index created")
    for community in communities:
        logger.debug("community %d/%d; %d speakers"%(i,len(communities), len(communities[community])))
        i+=1
        db['communities'].insert({'id': community, 'users': communities[community]})
        for user in communities[community]:
            db['users'].update({'id':user},{'$set':{'community':community}})
            #db['users'].insert({'id':user, 'community':community})

    logger.info("done.")

if __name__ == '__main__':
    main()
