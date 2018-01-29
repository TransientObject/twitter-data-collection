import tweepy
import config
import json
from datetime import datetime, date, time, timedelta

def get_travel_tweets_from_location(coordinates, geotag):
    end_date = datetime.utcnow() - timedelta(days=1)
    try:
        travelTweets = tweepy.Cursor(api.search, q='ttot', geocode=coordinates).items(200000)
        with open('data2/'+geotag+'.txt', "a+") as f:
            for tweet in travelTweets:
                if (tweet.created_at < end_date):
                    break
                json.dump(tweet._json, f)
                f.write("\n")
    except Exception as e:
        print("Exception Message - {}".format(str(e)))


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_key, config.access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    geocode = {}

    get_travel_tweets_from_location("40.7128,-74.0060,20km", "NYC")