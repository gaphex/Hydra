#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'
from datetime import datetime
import tweepy
import json
import meta
import sys

class Tweet():
    def __init__(self, json, tID = None, uID = None, txt = None, src = None, cat = None, timezone = None, location = None, geodata = None):
        self.text = txt
        self.device = src
        self.userID = uID
        self.tweetID = tID
        self.json = json
        self.createdAt = cat
        self.geodata = geodata
        self.location = location
        self.timezone = timezone
        self.sentiment = ''

class CustomStreamListener(tweepy.StreamListener):

    def __init__(self, dataHandler, streamHandler, pID, pDesc):

        self.pID = pID
        self.pDesc = pDesc
        self.dataHandler = dataHandler
        self.streamHandler = streamHandler
        super(CustomStreamListener, self ).__init__()

    def on_status(self, status):
        try:
            '''
            tid = status.id_str
            cat = status.created_at
            txt = status.text
            src = status.source
            geodata = status.coordinates
            usr = status.author.screen_name
            location = status.author.location
            timezone = status.author.time_zone'''
            tweet = Tweet(status._json)
            self.dataHandler.handleNewTweet(self.pID, self.pDesc, tweet)

        except Exception as e:
            print e

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

    def on_disconnect(self, notice):
        print notice
        return True # Don't kill the stream

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            encoded_object = list(obj.timetuple())[0:6]
        else:
            encoded_object =json.JSONEncoder.default(self, obj)
        return encoded_object