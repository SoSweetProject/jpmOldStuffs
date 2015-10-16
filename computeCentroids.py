import argparse
import logging
from collections import defaultdict
import ujson
import sys
import inspect
import pymongo
import numpy as np


logger = logging.getLogger(__name__)
logger_debug_frequency = 10000


def parseArgs():
    parser = argparse.ArgumentParser(description='Build a dictionary, where keys are users and value the GPS coordinates of their tweet.')
    parser.add_argument('--mongoDatabase', '-m', required=True, help='name of the mongo database. Host is assumed to be localhost, port 27017 and collection "users".')
    parser.add_argument('--output-file', '-o', required=True, help='path to the output file')
    parser.add_argument('--log-level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    return parser.parse_args(), parser


def computeUsersCentroids(coordinates):
    logger.info("computing users centroids")
    for community in coordinates:
        coordinates[community]=[computeCentroid(coordinates[community][user]) for user in coordinates[community]]
    logger.info("done")
    return coordinates


def computeCentroid(coordinates):
    #longitude first !
    #http://stackoverflow.com/questions/6671183/calculate-the-center-point-of-multiple-latitude-longitude-coordinate-pairs
    pts=np.radians(coordinates)
    x=np.average(np.cos(pts[:,1])*np.cos(pts[:,0]))
    y=np.average(np.cos(pts[:,1])*np.sin(pts[:,0]))
    z=np.average(np.sin(pts[:,1]))
    center_lon = np.arctan2(y,x)
    hyp = np.sqrt(x * x + y * y)
    center_lat = np.arctan2(z, hyp)
    coo = (np.degrees(center_lon),np.degrees(center_lat))
    return (np.degrees(center_lon),np.degrees(center_lat))

def computeAverageDistanceToCentroid(points, centroid):
    earth_radius = 6371
    coordinates=np.array(points)
    dlat = np.radians(coordinates[:,1]) - np.radians(centroid[1])
    dlon = np.radians(coordinates[:,0]) - np.radians(centroid[0])
    h = np.square(np.sin(dlat/2.0)) + np.cos(np.radians(centroid[1])) * np.cos(np.radians(coordinates[:,1])) * np.square(np.sin(dlon/2.0))
    great_circle_distance = np.arcsin(np.minimum(np.sqrt(h), np.repeat(1, len(h))))
    d = earth_radius * great_circle_distance
    return np.average(d)

def computeAverageDistancesToCentroids(coordinates,centroids):
    averageDistances={}
    for userOrCommunuty in coordinates:
        averageDistances[userOrCommunuty]=computeAverageDistanceToCentroid(coordinates[userOrCommunuty],centroids[userOrCommunuty])
    return averageDistances

def getCoordinates(database):
    uri = "mongodb://%s:%d/%s"%("localhost", 27017, database)
    logger.info("Connecting to %s"%uri)
    db = pymongo.MongoClient(uri)[database]
    logger.info("Connected")

    logger.debug("Ensuring tweets.user index")
    db.tweets.ensure_index('user')
    logger.debug("Ensuring tweets.latitude index")
    db.tweets.ensure_index('latitude', sparse=True)
    logger.debug("Ensuring tweets.longitude index")
    db.tweets.ensure_index('longitude', sparse=True)
    logger.debug("done")

    coordinates = defaultdict(lambda : defaultdict(list))
    logger.debug("Retrieving geolocalized tweets")
    tweets = db.tweets.find({'latitude' : {'$exists':True}},{'user':True, 'latitude':True, 'longitude':True}).hint([('latitude', 1)])
    nTweets=tweets.count()
    tweets.rewind()
    logger.info("%d geolocalized tweets"%nTweets)
    i=0
    cache={}
    for tweet in tweets:
        if i%logger_debug_frequency==0:
            logger.debug("%d/%d tweets treated"%(i,nTweets))
        i+=1
        user = tweet['user']
        if user not in cache:
            try:
                community = db.users.find_one({'id':user})['community']
                cache[user]=community
            except:
                continue
        else:
            community=cache[user]
# the first version is wrong as longitude must be given first
#        coordinates[community][user].append((tweet['latitude'],tweet['longitude']))
        coordinates[community][user].append((tweet['longitude'],tweet['latitude']))
    return coordinates

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

    coordinates = getCoordinates(args.mongoDatabase)
    coordinates = computeUsersCentroids(coordinates)

    logger.info("writing output to %s"%args.output_file)
    file=open(args.output_file,'w')
    for community in coordinates:
        for user in coordinates[community]:
            file.write("%d,%f,%f\n"%(community,user[0],user[1]))
    file.close()
    logger.info("done")

if __name__ == '__main__':
    main()
