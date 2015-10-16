import logging
import sqlite3
import glob
import datetime
import bokeh.plotting as bokeh_plt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as matplotlib_plt
from matplotlib.dates import DateFormatter
import itertools
import numpy as np


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
handler = logging.FileHandler('stats.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

path_to_data = "/datastore/complexnet/twitter/data/"
path_to_graphs = "/datastore/complexnet/twitter/graphs/"
# path_to_data = "/Users/jmague/Documents/Projets/SoSweet/data/"
# path_to_graphs = "/Users/jmague/Documents/Projets/SoSweet/graphs/"

stats_db_file_name = path_to_data+"stats.db"
users_db_file_name = path_to_data+"users.db"


set_last_treated_file_query = 'UPDATE misc SET value=? WHERE key="last_treated_tweets_file"'


# == SQLite stat_db_connection ==
stat_db_connection = sqlite3.connect(stats_db_file_name)
stat_db_connection.row_factory = sqlite3.Row
stat_db_cursor = stat_db_connection.cursor()


def iterFilesNames(fromFile, toFile, includeFromFile=False):
    fromDate = datetime.datetime.strptime(fromFile.split('/')[-1].split('.')[0], "%Y-%m-%dT%H")
    toDate = datetime.datetime.strptime(toFile.split('/')[-1].split('.')[0], "%Y-%m-%dT%H")
    logger.info("Iterating throuhg files names from %s (%s)" % (fromFile, "included" if includeFromFile else "excluded"))
    logger.info("Iterating throuhg files names to   %s (included)" %toFile)
    directory = '/'.join(fromFile.split('/')[:-1])+'/'
    if includeFromFile:
      currentDate = fromDate
    else:
      currentDate = fromDate + datetime.timedelta(hours=1)
    while currentDate<= toDate:
        yield directory + datetime.datetime.strftime(currentDate, "%Y-%m-%dT%H") + '.data'
        currentDate += datetime.timedelta(hours=1)


def tweets():
    logger.info("computing stats on tweets")
    files = glob.glob(path_to_data+'*.data')
    files.sort()
    if len(files) <= 2:
        return
    last_file_to_treat = files[-3]  # we don't treat the last two files as they may me modified by the dataCollection process
    stat_db_cursor.execute('SELECT value FROM misc WHERE key="last_treated_tweets_file"')
    try:
        last_treated_tweets_file = stat_db_cursor.fetchone()[0]
        files_to_treat = iterFilesNames(last_treated_tweets_file, last_file_to_treat, includeFromFile=False)
    except TypeError:
        files_to_treat = iterFilesNames(files[0], last_file_to_treat, includeFromFile=True)
    for file in files_to_treat:
        logger.info("treating %s" % file)
        try:
            # number of tweets in the current hour
            num_tweets = sum(1 for line in open(file))
        except IOError:
            logger.info("no such file: %s" % file)
            num_tweets = 0
        logger.info("Number of tweets: %d" % num_tweets)
        # cumulative volume
        hour = datetime.datetime.strptime(file.split('/')[-1].split('.')[0], "%Y-%m-%dT%H")
        stat_db_cursor.execute("SELECT cumulative FROM tweets WHERE date = ?", ((hour-datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H"),))
        try:
            previous_cumulative_volume = stat_db_cursor.fetchone()[0]
        except TypeError:
            previous_cumulative_volume = 0
        cumulative_volume = previous_cumulative_volume + num_tweets
        logger.info("Cumulative volume:  %d" % cumulative_volume)
        # volume in the past 24 hours
        volumePast24h = 0
        for h in range(1, 25):
            stat_db_cursor.execute("SELECT current_hour FROM tweets WHERE date = ?", ((hour-datetime.timedelta(hours=h)).strftime("%Y-%m-%dT%H"),))
            try:
                volume = stat_db_cursor.fetchone()[0]
            except TypeError:
                volume = 0
            volumePast24h += volume
        logger.info("Volume in the past 24 hours:  %d" % volumePast24h)
        stat_db_cursor.execute("INSERT INTO tweets VALUES (?,?,?,?)", (hour.strftime("%Y-%m-%dT%H"), num_tweets, cumulative_volume, volumePast24h))
        stat_db_cursor.execute('UPDATE misc SET value=? WHERE key="last_treated_tweets_file"', (file,))
        stat_db_connection.commit()


def users():
    logger.info("computing stats on users")
    users_db_connection = sqlite3.connect(users_db_file_name)
    users_db_connection.row_factory = sqlite3.Row
    users_db_cursor = users_db_connection.cursor()
    users_db_cursor.execute("SELECT count(*) FROM users")
    nUsers = users_db_cursor.fetchone()[0]
    users_db_cursor.execute("SELECT count(*) FROM users WHERE friends is not NULL")
    nUsersWithFriends = users_db_cursor.fetchone()[0]
    date = datetime.datetime.utcnow().isoformat()
    stat_db_cursor.execute("INSERT INTO users VALUES (?,?,?)", (date, nUsers, nUsersWithFriends))
    stat_db_connection.commit()


def tweetsGraph():
    logger.info("Drawing graphs to %s" % path_to_graphs+"Stats.html")
    stat_db_cursor.execute('SELECT * FROM tweets')
    tweets = stat_db_cursor.fetchall()
    date, volume, cumulative, volumePast24h = zip(*[(datetime.datetime.strptime(t['date'], "%Y-%m-%dT%H"), t['current_hour'], t['cumulative'], t['past_24h']) for t in tweets])
    hourly =zip([datetime.datetime(year=d.year, month=d.month, day=d.day) for d in date],volume)
    hourly.sort()
    days, dailyVolume = zip(*[(d, sum([v[1] for v in vol])) for d,vol in itertools.groupby(hourly, lambda i:i[0])])

    bokeh_plt.output_file(path_to_graphs+"Stats.html")
    bokeh_plt.hold()
    bokeh_plt.quad(days, [d+datetime.timedelta(days=1) for d in days], dailyVolume, [0]*len(dailyVolume),  x_axis_type="datetime", color='gray', legend="Daily volume")
    bokeh_plt.line(date, volume, x_axis_type="datetime",  color='red', legend="Hourly volume")
    bokeh_plt.line(date, volumePast24h, x_axis_type="datetime", color='green', legend="Volume in the past 24 hours")
    bokeh_plt.curplot().title = "Volume"
    bokeh_plt.figure()
    bokeh_plt.line(date, cumulative, x_axis_type="datetime")
    bokeh_plt.curplot().title = "Cumulative volume"

    fig, ax = matplotlib_plt.subplots()
    f=DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(f)
    matplotlib_plt.plot(date, volume)
    matplotlib_plt.plot(date, volumePast24h)
    matplotlib_plt.plot(days, dailyVolume)
    matplotlib_plt.xticks(np.concatenate((np.array(date)[range(0,len(date),24*7)],[date[-1]])), rotation=70)
    matplotlib_plt.savefig(path_to_graphs+"volume.png", bbox_inches="tight")


    stat_db_cursor.execute('SELECT * FROM users')
    users = stat_db_cursor.fetchall()
    date, nUsers, nUsersWithFriends = zip(*[(datetime.datetime.strptime(u['date'], "%Y-%m-%dT%H:%M:%S.%f"), u['total'], u['with_friends']) for u in users])
    bokeh_plt.figure()
    bokeh_plt.line(date, nUsers, x_axis_type="datetime", legend="Total")
    bokeh_plt.line(date, nUsersWithFriends, x_axis_type="datetime", legend="Friendship collected")
    bokeh_plt.legend().orientation = "top_left"
    bokeh_plt.curplot().title = "Number of users"
    bokeh_plt.save()

    matplotlib_plt.figure()
    fig, ax = matplotlib_plt.subplots()
    f=DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(f)
    matplotlib_plt.plot(date, nUsers)
    matplotlib_plt.plot(date, nUsersWithFriends)
    matplotlib_plt.xticks(np.concatenate((np.array(date)[range(0,len(date),24*7)],[date[-1]])), rotation=70)
    matplotlib_plt.savefig(path_to_graphs+"users.png", bbox_inches="tight")


def main():
    tweets()
    users()
    tweetsGraph()


if __name__ == '__main__':
    main()
