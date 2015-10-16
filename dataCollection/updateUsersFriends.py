import tweepy
import ujson
import sqlite3
import datetime
import logging
import time
import threading
import Queue
import os
import signal
import argparse

logger = logging.getLogger(__name__)

consumer_key = 'oaJdaj6xWQVcpMgBJC0VA'
consumer_secret = '2zAfnaIDbGO6vXesL3omTl6N9pOOpvkZZDsVyGgfM0'


class FriendsFetcher(threading.Thread):

    def __init__(
            self, name, access_token_key, access_token_secret, usersToTreatQueue, usersTreatedQueue):
        threading.Thread.__init__(self)
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token_key, access_token_secret)
        self.twitter = tweepy.API(auth)
        self.user = None
        self.usersToTreatQueue = usersToTreatQueue
        self.usersTreatedQueue = usersTreatedQueue
        self.__stop = False
        self.name = name
        logger.info("Worker %s created" % self.name)

    def __fetchUserFriends(self):
        if not self.user:
            raise Exception
        try:
            pages = tweepy.Cursor(
                self.twitter.friends_ids,
                user_id=self.user['id'],
                count=5000).pages()
            friends = []
            self.__waitUntilNextRequest()
            for page in pages:
                for friend in page:
                    friends.append(friend)
                self.__waitUntilNextRequest()
            self.user['friends'] = friends
        except tweepy.TweepError as e:
            logger.exception("Exception in %s" % (self.name))
            self.user['friends'] = e.response.reason
        except Exception as e:
            logger.exception("Exception in %s" % (self.name))
            self.user['friends'] = 'unknown error'
        finally:
            self.usersTreatedQueue.put(self.user)

    def __waitUntilNextRequest(self):
        status = self.twitter.rate_limit_status()['resources']['friends']['/friends/ids']
        if status['remaining'] < 1:
            t = status['reset'] - time.time() + 10  # we wait 10 more seconds to be sure
            logger.info("%s is sleeping for %.0f minutes and %.0f seconds" % ((self.name,) + divmod(t, 60)))
            time.sleep(t)
            logger.info("%s is waking up" % (self.name))

    def run(self):
        self.user = self.usersToTreatQueue.get()
        while self.user is not None:
            logger.info("%s is fetching friends of %s (%d)" % (self.name, self.user['screen_name'], self.user['id']))
            self.__fetchUserFriends()
            self.usersToTreatQueue.task_done()
            logger.info("%s is done. %d friends collected for %s (%d)" % (self.name, len(self.user['friends']), self.user['screen_name'], self.user['id']))
            self.user = self.usersToTreatQueue.get()
            if self.user is None:
                logger.debug("%s's user is None"%self.name)
        logger.info("%s is stopping." % (self.name))


def signal_handler(signum, frame):
    logger.critical('Signal handler called with signal %s' % str(signum))
    raise KeyboardInterrupt()


def parseArgs():
    parser = argparse.ArgumentParser(description="Fetch users' friends from Twitter.")
    parser.add_argument('--database', '-d', required=True, help='path to the database file')
    parser.add_argument('--log_level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    return parser.parse_args()


def createWorkers(cursor, usersToTreatQueue, usersTreatedQueue):
    cursor.execute('SELECT * FROM twitter_accounts')
    accounts = cursor.fetchall()
    logger.info("Using %d twitter accounts" % len(accounts))
    logger.info("creating workers...")
    workers = []
    for account in accounts:
        worker = FriendsFetcher(name=account['name'], access_token_key=account['access_token_key'], access_token_secret=account['access_token_secret'], usersToTreatQueue=usersToTreatQueue, usersTreatedQueue=usersTreatedQueue)
        workers.append(worker)
        worker.start()
    return workers


def getUsersToTreat(cursor):
    cursor.execute('SELECT * FROM users where friends is NULL')
    users = cursor.fetchall()
    return users


def insertUsers(connection, cursor, usersTreatedQueue):
    logger.info("inserting users' friends in database")
    nUsers = 0
    while not usersTreatedQueue.empty():
        user = usersTreatedQueue.get()
        date = datetime.datetime.utcnow().isoformat()
        cursor.execute('UPDATE users SET date=?, friends=? WHERE id=?', (date, ujson.dumps(user['friends']), user['id']))
        logger.debug("inserting %d friends for %s at %s in database" % (len(user['friends']), user['screen_name'], date))
        nUsers += 1
    connection.commit()
    logger.info('commited : %d inserted' % nUsers)


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    args = parseArgs()

    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.FileHandler('updateUsersFriends.log')
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    logger.info("Opening database %s" % args.database)
    connection = sqlite3.connect(args.database)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    usersToTreatQueue = Queue.Queue()
    usersTreatedQueue = Queue.Queue()

    workers = createWorkers(cursor, usersToTreatQueue, usersTreatedQueue)
    try:
        usersToTreat = getUsersToTreat(cursor)
        logger.info("%d users to treat" % len(usersToTreat))
        for user in usersToTreat:
            usersToTreatQueue.put(dict(user))
        while not(usersToTreatQueue.empty()):
            logger.debug("%d users waiting for friends fetching" % usersToTreatQueue.qsize())
            if usersTreatedQueue.qsize()>0:
                insertUsers(connection, cursor, usersTreatedQueue)
            time.sleep(10)

        logger.info("all users have been treated")
        for worker in workers:
            usersToTreatQueue.put(None)
        connection.close()

    except KeyboardInterrupt:
        logger.warning("Script interupted. Terminating.")
        while not usersToTreatQueue.empty():
            usersToTreatQueue.get()
        logger.warning("queue cleared.")
        insertUsers(connection, cursor, usersTreatedQueue)
        connection.close()
        pid = os.getpid()
        logger.critical("Killing current process: %d" % pid)
        os.kill(pid, 1)

if __name__ == '__main__':
    main()
