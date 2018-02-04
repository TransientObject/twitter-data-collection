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
    for tup in [("39.1911,-106.8175,20km", "Aspen"),("42.3601,-71.0589","Boston"),
                ("39.4817,-106.0384,20km","Breckenridge"),("32.7765,-79.9311,20km","Charleston"),
                ("41.8781,-87.6298", "Chicago"), ("39.7392,-104.9903","Denver"),
                ("42.3314,-83.0458","Detroit"),("21.3069,-157.8583","Honolulu"),
                ("25.7617,-80.1918","Miami"),("39.9526,-75.1652","Philadelphia"),
                ("33.4484,-112.0740","Phoenix"), ("40.4406,-79.9959","Pittsburgh"),
                ("33.4942,-111.9261", "Scottsdale"), ("47.6062,-122.3321", "Seattle")]:
                # ("40.7128,-74.0060,20km", "NYC"),("39.9526,-75.1652,20km", "Philadelphia"),
                # ("21.3069,-157.8583,20km","Honolulu"),("20.7984,-156.3319,20km","Maui"),
                # ("37.7749,-122.4194,20km","San Francisco"),("36.0544,-112.1401,20km","Grand Canyon"),
                # ("38.2919,-122.4580,20km","Sonoma"),("38.9072,-77.0369,20km","Washington D.C"),
                # ("29.9511,-90.0715,20km","New Orleans"),("32.7157,-117.1611,20km","San Diego"),
                # ("22.0964,-159.5261,20km","Kauai"),("19.8968,-155.5828,20km","Hawaii"),
                # ("39.4817,-106.0384,20km","Breckenridge"),("42.3601,-71.0589,20km","Boston"),
                # ("25.7617,-80.1918,20km","Miami"),("34.0522,-118.2437,20km","Los Angeles"),
                # ("32.7765,-79.9311,20km","Charleston"),("37.8651,-119.5383,20km","Yosemite"),
                # ("47.6062,-122.3321,20km","Seattle"),("35.5951,-82.5515,20km","Asheville, NC"),
                # ("40.6461,-111.4980,20km","Park City, Utah"),("39.7392,-104.9903,20km","Denver"),
                # ("32.0835,-81.0998,20km","Savannah"),("36.3615,-121.8563,20km","Big Sur"),
                # ("44.4280,-110.5885,20km","Yellowstone"),("41.8781,-87.6298", "Chicago"),
                # ("36.1699,-115.1398", "Las Vegas"),("45.512794,-122.679565,20km","Portland"),
                # ("39.8309°,-77.2311,20km", "Gettysburg"),("37.8651,-119.5383,20km","Yosemite")]:
        store_travel_tweets_from_location(tup[0], tup[1])