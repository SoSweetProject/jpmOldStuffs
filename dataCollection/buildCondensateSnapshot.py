import ujson
import glob
import argparse
import logging
import datetime
import tarfile
import sys

logger = logging.getLogger(__name__)


def parseArgs():
    parser = argparse.ArgumentParser(
        description='Create a set of files containg the text of each tweet, its user and its date.')
    parser.add_argument(
        '--path_to_input_data', '-p', required=True, help='path to input data files')
    parser.add_argument(
        '--path_to_output_data', '-o', required=True, help='path to output data files')
    parser.add_argument('--log-level', '-l', default='info',
                        help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', default='file', help='logger destination', choices=['file', 'stderr'])
    parser.add_argument(
        '--date', '-d', default=None, help='Date of the snapshot. Format: 2014-09-01')
    parser.add_argument('--language', '-L', default=True,
                        help='Include language identification information', type=bool)
    parser.add_argument('--coordinates', '-c', default=True,
                        help='Include geolocation information', type=bool)
    parser.add_argument('--geolocalized-only', '-g', default=False,
                        help='Include only geolocalized tweets', type=bool)
    args = parser.parse_args()
    if args.path_to_input_data == args.path_to_output_data:
        raise ValueError(
            "paths to input data files and output data files must be different")
    print args
    return args


def getFilesToTreat(path, date):
    logger.debug("looking for files to treat")
    if date is None:
        date = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
    files = [f for f in glob.glob(path + '*.data') if f.split('/')[-1].split('T')[0] <= date]
    logger.debug("data files:")
    logger.debug("\n".join(files))
    for fileName in [f for f in glob.glob(path + '*.tgz') if f.split('/')[-1].split('.')[0] <= date]:
        logger.debug("checking:%s"%fileName)
        tf=tarfile.open(fileName)
        filesOK=[(n,tf) for n in tf.getnames() if n.split('/')[-1].split('T')[0] <= date and n.split('.')[-1] == "data"]
        logger.debug("\n".join([f[0] for f in filesOK]))
        files+=filesOK
    files.sort()
    logger.info("%d files to treat"%len(files))
    # logger.debug("\n".join([f if files))
    return files


def treatFile(input_file_name, path_to_output_data, language, coordinates, geolocalized_only):
    if type(input_file_name) == str:
        logger.debug("treating %s"%input_file_name)
        fileIn = open(input_file_name)
        output_file_name=path_to_output_data + input_file_name.split('/')[-1]
        fileOut = open(output_file_name, "w")
    else :
        logger.debug("treating %s"%input_file_name[0])
        logger.debug("extracting %s"%input_file_name[0])
        fileIn = input_file_name[1].extractfile(input_file_name[0])
        output_file_name=path_to_output_data + input_file_name[0].split('/')[-1]
        fileOut = open(output_file_name, "w")
    logger.debug("output file: %s"%output_file_name)
    for l in fileIn:
        try:
            tweet = ujson.loads(l)
        except ValueError:
            continue
        if geolocalized_only and "geo" not in tweet['twitter']:
            continue
        condensate = {"tweet": tweet['twitter']['text'], 'date': tweet[
            'twitter']['created_at'], 'user': tweet['twitter']['user']['id'], 'id':tweet['twitter']['id']}
        if coordinates and "geo" in tweet['twitter']:
            condensate['latitude']=tweet['twitter']["geo"]['latitude']
            condensate['longitude']=tweet['twitter']["geo"]['longitude']
        if language:
            condensate['language']={}
            condensate['language']['user']=tweet['twitter']['user']['lang']
            condensate['language']['twitter']=tweet['twitter']['lang']
            if 'language' in tweet:
                condensate['language']['datasift']={}
                condensate['language']['datasift']['language']=tweet['language']['tag']
                condensate['language']['datasift']['confidence']=tweet['language']['confidence']
        fileOut.write(ujson.dumps(condensate) + "\n")
    fileOut.close()


def main():
    args = parseArgs()

    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log_destination == 'file':
        handler = logging.FileHandler('buildCondensateSnapshot.log')
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    if args.path_to_input_data[-1] != '/':
        args.path_to_input_data += '/'
    if args.path_to_output_data[-1] != '/':
        args.path_to_output_data += '/'
    logger.info("input directory: %s"%args.path_to_input_data)
    logger.info("output directory: %s"%args.path_to_output_data)

    i=0
    files=getFilesToTreat(args.path_to_input_data, args.date)
    for file in files:
        if i%10==0:
            logger.info("%d/%d files treated"%(i,len(files)))
        logger.info('treating ' + file if type(file)==str else file[0])
        treatFile(file, args.path_to_output_data, args.language, args.coordinates, args.geolocalized_only)
        i+=1

if __name__ == '__main__':
    main()
