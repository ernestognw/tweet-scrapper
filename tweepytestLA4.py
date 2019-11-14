#!/usr/bin/python
# Enumerar Searchs
import tweepy
import json
import time
import sys
import os

# override tweepy.StreamListener
from tweepy import API

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, api=None):
        super(MyStreamListener, self).__init__(api)
        self.api = api or API()
        self.counter = 0
        self.researcherID = "3";
        self.searchID = "1"; #1,2,3
        # define the filename with time as prefix
        self.output = open('bdatweets_%s.json'
                        % (time.strftime('%Y%m%d-%H%M%S')), 'a')
        # researcher ID and searchID
        self.output.write(self.researcherID+'\n'+self.searchID+'\n')
    def on_status(self, status):
        self.counter += 1
        json.dump(status._json, self.output)
        self.output.write('\n')
        if self.counter >= 2300:
            self.output.close()
            self.output = open('bdatweets_%s.json'
                                % (time.strftime('%Y%m%d-%H%M%S')), 'a')
            # researcher ID and searchID
            self.output.write(self.researcherID+'\n'+self.searchID+'\n')
            self.counter = 0
        return

    def on_error(self, status):
        print status

from dotenv import load_dotenv
load_dotenv(dotenv_path='app.env', verbose=True)
consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener(api))
print('Reading Twitter Stream...')
myStream.filter(track=['EASports', 'FIFA20', 'Fut Champions'], languages=["en"])
