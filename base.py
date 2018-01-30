import tweepy
import config
import json
from datetime import datetime, date, time, timedelta
import sqlite3
from TweetInfo import TweetInfo
import collections

DBNAME = 'twitter.db'
conn = sqlite3.connect(DBNAME)
c = conn.cursor()

def create_table_if_not_exists():
    c.execute('''CREATE TABLE IF NOT EXISTS tweets
        (tweetText text,
        user_profile text,
        user_id text,
        date text,
        tweet_location text,
        search_location text,
        tweet_id text PRIMARY KEY,
        tweet_json text)''')
    conn.commit()
    conn.close()

def get_max_id_for_location(location):
    conn = sqlite3.connect(DBNAME)
    c = conn.cursor()
    c.execute("SELECT min(tweet_id) FROM tweets where search_location = ?", [location])
    rows = c.fetchall()
    return rows[0][0]
    conn.close()


def store_travel_tweets_from_location(coordinates, geotag):
    try:
        max_id = get_max_id_for_location(geotag)
        item_count = 5
        if max_id is None:
            travelTweets = tweepy.Cursor(api.search, q='ttot', geocode=coordinates).items(item_count)
        else:
            max_id = str(int(max_id) - 1)
            travelTweets = tweepy.Cursor(api.search, q='ttot', geocode=coordinates, max_id = max_id).items(item_count)

        for tweet in travelTweets:
            status_string = json.dumps(tweet._json)
            status_json = json.loads(status_string)
            tweet_info = TweetInfo(status_json['text'], 'https://twitter.com/' + status_json['user']['screen_name'], status_json['user']['id_str'], status_json['created_at'], status_json['user']['location'], geotag, status_json['id'], status_string)
            tweet_info.insertTweet()
    except Exception as e:
        print("Exception Message - {}".format(str(e)))
    finally:
        print("tweet scrapping process done for location - " + geotag + "\n")


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_key, config.access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    create_table_if_not_exists()
    TweetInfo.init_connection_to_db(DBNAME)
    for tup in [("40.7128,-74.0060,20km", "NYC"),("4.7128,-7.0060,20km", "LA"),("80.7128,-4.0060,20km", "ORLANDO")]:
        store_travel_tweets_from_location(tup[0], tup[1])