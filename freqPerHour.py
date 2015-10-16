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


    freq = {}

    for hour in range(24):
        print hour
        project = {
            '$project': {
                "hour": {
                    "$hour": "$twitter.created_at"},
                "text": '$twitter.text'}}
        match = {'$match': {'hour': hour}}
        res = db.tweets.aggregate([project, match])['result']
        texts = [wordpunct_tokenize(tweet['text']) for tweet in res]
        freq[hour] = FreqDist([word for text in texts for word in text])

f = open('../../weather/data/freqPerHour.pickle', 'w')
cPickle.dump(freq, f)
f.close()

totalFreq = freq[0]
for i in range(1, 24):
    totalFreq += freq[i]

tokens = totalFreq.keys()
i = 0
for token in tokens:
    freqs = [freq[h][token] / freq[h].N() for h in freq]
    db.freqs.save({'token': token, 'freqs': freqs, 'var': np.var(freqs)})
    if i % 1000 == 0:
        print i
    i += 1


def plotWordFreq(db, words):
    if not isinstance(words, list):
        words = [words]
    freqs = [np.roll(db.freqs.find_one(
        {'token': word}, {'freqs': 1})['freqs'], 2) for word in words]
    for i in range(len(freqs)):
        plt.plot(freqs[i], label=words[i])
    plt.legend()
    plt.show()



if __name__ == '__main__':
    main()
