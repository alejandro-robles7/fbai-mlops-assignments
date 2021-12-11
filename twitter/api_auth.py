# Import package
import tweepy

# Store OAuth authentication credentials in relevant variables
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""

# Pass OAuth details to tweepy's OAuth handler
auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)

