#!/usr/bin/python

import tweepy
import json
import time
import sys
import glob
import mysql.connector
import os

from dotenv import load_dotenv
load_dotenv(dotenv_path='app.env', verbose=True)


mydb = mysql.connector.connect(
  host="192.168.64.2",
  user= os.getenv("MYSQL_USER"),
  passwd= os.getenv("MYSQL_PWD"),
  database="TwitterProject",
)
mydb.set_charset_collation('utf8mb4')

print(mydb)
cursor = mydb.cursor()
cursor.execute("SELECT * FROM RESEARCHERS")
rows = cursor.fetchall()

# read all json files
file_str = r'bdatweets_*.json'
# list of pathnames according to above regex
file_lst = glob.glob(file_str)

# process every file
for file_idx, file_name in enumerate(file_lst):
    counter = 0
    with open(file_name, 'r') as f:
        for line in f:
            if counter == 0:
                # read researcher ID from the first line
                researcherID = int(line)
                counter = counter + 1
                continue
            if counter == 1:
                # read search ID from the second line
                searchID = int(line)
                counter = counter + 1
                continue
            if line != '\n':
                # each line is a tweet json object, load it and display user id
                tweet = json.loads(line)
                # user info
                userID = tweet['user']['id']
                followers_count = tweet['user']['followers_count']
                description = tweet['user']['']
		        location = tweet['user']['location']
		        friends_count = tweet['user']['friends_count']
                profile_text_color = tweet['user']['profile_text_color']
                verified = tweet['user']['verified']
                name = tweet['user']['name']
                
                # tweet info
                tweet_id = tweet['id']
                tweet_text = tweet['text']
                favorite_count = tweet['favorite_count']
                # read hashtag information [indices, text]
                hashtag_objects = tweet['entities']['hashtags']
                # insert only if the user doesn't exists already in the database
                cursor.execute('SELECT * FROM USERS WHERE id = %s', (userID,))
                rows = cursor.fetchall()
                if len(rows) == 0:
                    cursor.execute('''
                        INSERT INTO USERS (id, verified, followers_count)
                            VALUES
                                (%s,%s,%s)
                    ''', (userID, verified, followers_count))
                    mydb.commit()
                # insert tweet object
                cursor.execute(''' SELECT id from TWEETS WHERE id= %s
                    INSERT INTO TWEETS (id, tweet_text, user, favorite_count, search_id)
                        VALUES
                            (%s,%s,%s,%s,%s)
                ''', (tweet_id, tweet_text, userID, favorite_count, searchID))
                mydb.commit()
                # insert hashtags
                for hashtag in hashtag_objects:
                    # insert only unique hashtags
                    cursor.execute('''SELECT * FROM HASHTAGS WHERE tweet_id = %s AND
                            hashtag = %s''', (tweet_id, hashtag['text']))
                    rows = cursor.fetchall()
                    if len(rows) == 0:
                        cursor.execute('''
                            INSERT INTO HASHTAGS (tweet_id, hashtag)
                                VALUES(%s,%s)
                        ''', (tweet_id, hashtag['text'],))
                        mydb.commit()
cursor.close()
mydb.close()
