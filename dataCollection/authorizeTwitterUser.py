import tweepy
import sqlite3

consumer_key = 'oaJdaj6xWQVcpMgBJC0VA'
consumer_secret = '2zAfnaIDbGO6vXesL3omTl6N9pOOpvkZZDsVyGgfM0'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

try:
    redirect_url = auth.get_authorization_url()
except tweepy.TweepError:
    print 'Error! Failed to get request token.'

print redirect_url
verifier = raw_input('Verifier:')
try:
    auth.get_access_token(verifier)
except tweepy.TweepError:
    print 'Error! Failed to get access token.'

twitter = tweepy.API(auth)
screen_name = twitter.me().screen_name

print screen_name
print auth.access_token.key
print auth.access_token.secret


u = {'name': screen_name,
     'access_token_key': auth.access_token.key,
     'access_token_secret': auth.access_token.secret}

print u


db_file_name = "users.db"
insert_twitter_account_query = 'INSERT INTO twitter_accounts VALUES (?,?,?)'
connection = sqlite3.connect(db_file_name)
cursor = connection.cursor()
cursor.execute(insert_twitter_account_query, (u['name'], u['access_token_key'], u['access_token_secret']))
connection.commit()
