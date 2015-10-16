import glob
import sqlite3
import ujson
import logging
import argparse
import sys

logger = logging.getLogger(__name__)


def getFilesToTreat(cursor, path_to_data, last_treated_file=None):
    if not last_treated_file:
        cursor.execute('SELECT value FROM misc WHERE key="last_treated_file"')
        try:
            last_treated_file = cursor.fetchone()[0]
        except TypeError:
            last_treated_file = None
    logger.info('last treated file : %s' % last_treated_file)
    files = glob.glob(path_to_data+'*.data')
    files.sort()
    try:
        files_to_treat = files[files.index(last_treated_file)+1:]
    except ValueError:
        files_to_treat = files
    files_to_treat = files_to_treat[:-2]  # we don't treat the last two files as they may me modified by the dataCollection process
    logger.info("%d file to treat" % len(files_to_treat))
    return files_to_treat


def treatFile(file_name, cursor, connection):
    logger.info('treating file %s' % file_name)
    file = open(file_name)
    ids_screen_names = {}
    for l in file:
        try:
            tweet = ujson.loads(l)
        except ValueError:
            continue
        ids_screen_names[tweet['twitter']['user']['id']]=tweet['twitter']['user']['screen_name']
    file.close()
    logger.info('%d users found in %s' % (len(ids_screen_names), file_name))
    newUsers = 0
    for id in ids_screen_names:
        cursor.execute('SELECT * FROM users where id=?', (id,))
        if cursor.fetchone() is None:
            logger.debug("%d was not in the database" % id)
            cursor.execute('INSERT INTO users VALUES (?,?,?,?)', (id, ids_screen_names[id], None, None))  # no friends, no date
            newUsers += 1
        else:
            logger.debug("%d was in the database" % id)
    cursor.execute('UPDATE misc SET value=? WHERE key="last_treated_file"', (file.name,))
    connection.commit()
    logger.info("%d new users from file %s" % (newUsers, file_name))
    logger.info('commited')


def parseArgs():
    parser = argparse.ArgumentParser(description='Extract unknown users from data files and store them in database.')
    parser.add_argument('--path_to_data', '-p', required=True, help='path to the data files')
    parser.add_argument('--database', '-d', required=True, help='path to the database file')
    parser.add_argument('--log_level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--last_treated_file', '-f', default=None, help='Last treated data file. If not supplied, information is drawn from the database')
    return parser.parse_args()


def main():
    args = parseArgs()

    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.FileHandler('updateUsersCollection.log')
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    # == SQLite connection ==
    logger.info("Opening database %s" % args.database)
    connection = sqlite3.connect(args.database)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    for file_name in getFilesToTreat(cursor, args.path_to_data, last_treated_file=args.last_treated_file):
        treatFile(file_name, cursor, connection)

    connection.close()
    logger.info("done.")

if __name__ == '__main__':
    main()
