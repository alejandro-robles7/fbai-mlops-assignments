import json
import tweepy


class MyStreamListener(tweepy.streaming.Stream):
    def __init__(self, file = 'tweets.txt', mode = 'w', api=None):
        super(MyStreamListener, self).__init__()
        self.num_tweets = 0
        self.file = open(file, mode)

    def on_status(self, status):
        tweet = status._json
        self.file.write(json.dumps(tweet) + '\n')
        self.num_tweets += 1
        if self.num_tweets < 100:
            return True
        else:
            return False
        self.file.close()