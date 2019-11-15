#!/usr/bin/python
from __future__ import unicode_literals
import tweepy
import json
import time
import sys
import os

# Get env variables
import os
from dotenv import Dotenv
dotenv = Dotenv(os.path.join(os.path.dirname(__file__), "../.env"))
os.environ.update(dotenv)

#override tweepy.StreamListener
from tweepy import API

searchId = 1

class MyStreamListener(tweepy.StreamListener):
    def __init__(self, api=None):
        super(MyStreamListener, self).__init__(api)
        self.api = api or API()
        self.startFile()
        
    def startFile(self):
      # define the filename with time as prefix
      self.path = os.path.join(os.path.dirname(__file__), './search-%d-%s.json' %(searchId, time.strftime('%Y%m%d-%H%M%S')))
      self.output = open(self.path, 'a')
      #resercher ID and searchID
      self.counter = 0

    def on_status(self, status):
        self.counter += 1
        print('Getting tweet %d' %(self.counter))
        json.dump(status._json, self.output)
        self.output.write('\n')
        if self.counter >= 2000:
            self.output.close()
            self.startFile()
        return

    def on_error(self, status):
        print(status)

consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener(api))
myStream.filter(track=['Apple'], locations=[-101.31,25.67,-100.31,26.67,-100.13, 19.43,-99.13, 20.43])