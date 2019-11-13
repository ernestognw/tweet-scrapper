#!/usr/bin/python

from __future__ import unicode_literals
import tweepy
import json
import time
import sys
import os


from dotenv import Dotenv
dotenv = Dotenv(os.path.join(os.path.dirname(__file__), ".env"))
os.environ.update(dotenv)

#override tweepy.StreamListener
from tweepy import API


class MyStreamListener(tweepy.StreamListener):

    def __init__(self, api = None):
        super(MyStreamListener, self).__init__(api)
        self.api = api or API()
        self.counter = 0
        # define the filename with time as prefix
        self.output  = open('bdatweets_%s.json' % (time.strftime('%Y%m%d-%H%M%S')), 'a')

        #resercher ID and searchID
        self.output.write('Researcher 4\n1\n')
    def on_status(self, status):
        self.counter += 1
        print('Reading Twitter Stream...')
        json.dump(status._json, self.output)
        self.output.write('\n')

        if self.counter >= 2000:
            self.output.close()
            self.output  = open('bdatweets_%s.json' % (time.strftime('%Y%m%d-%H%M%S')), 'a')
            
            #resercher ID and searchID
            self.output.write('Researcher 4\n1\n')
            self.counter = 0
        return

    def on_error(self, status):
        print (status)

consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
myStream = tweepy.Stream(auth = api.auth, listener=MyStreamListener(api))
myStream.filter(track=['Apple'], locations=[-101.31,25.67,-100.31,26.67,-100.13, 19.43,-99.13, 20.43])
