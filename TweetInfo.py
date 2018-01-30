import sqlite3

class TweetInfo():
    c = -1
    conn = -1

    # Data on the tweet
    def __init__(self, text, user, user_id, date, tweet_location, search_location, tweet_id, tweet_json):
        self.tweetText = text
        self.user_profile = user
        self.user_id = user_id
        self.tweet_id = tweet_id
        self.date = date
        self.tweet_location = tweet_location
        self.search_location = search_location
        self.tweet_json = tweet_json

    @staticmethod
    def init_connection_to_db(DBNAME):
        TweetInfo.conn = sqlite3.connect(DBNAME)
        TweetInfo.c = TweetInfo.conn.cursor()

    # Inserting that data into the DB
    def insertTweet(self):
        try:
            TweetInfo.c.execute("INSERT INTO tweets (tweetText, user_profile, user_id, date, tweet_location, search_location, tweet_id, tweet_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (self.tweetText, self.user_profile, self.user_id, self.date, self.tweet_location, self.search_location, self.tweet_id, self.tweet_json))
            TweetInfo.conn.commit()
        except Exception as e:
            print("Exception while inserting into db - " + str(e))