#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'
from datetime import datetime
import tweepy
import json
import sys

class Tweet():
    def __init__(self, tID, uID, txt, src, cat, timezone, location, geodatum):
        self.text = txt
        self.device = src
        self.userID = uID
        self.tweetID = tID
        self.createdAt = cat
        self.trueLocation = {}
        self.geodata = geodatum
        self.location = location
        self.timezone = timezone

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            encoded_object = list(obj.timetuple())[0:6]
        else:
            encoded_object =json.JSONEncoder.default(self, obj)
        return encoded_object

class CustomStreamListener(tweepy.StreamListener):

    def __init__(self, streamHandler, pID, batchSize):
        self.pID = pID
        self.size = batchSize
        super(CustomStreamListener, self ).__init__()
        self.streamHandler = streamHandler

    def on_status(self, status):
        try:
            tid = status.id_str
            cat = status.created_at
            txt = status.text.strip()
            src = status.source.strip()
            geodatum = status.coordinates
            timezone = status.author.time_zone
            usr = status.author.screen_name.strip()
            location = status.author.location.strip()
            tweet = Tweet(tid,usr,txt,src,cat,timezone,location,geodatum)
            self.streamHandler.handleNewTweet(self.pID, tweet)

        except Exception as e:
            # Most errors we're going to see relate to the handling of UTF-8 messages (sorry)
            self.streamHandler.refreshProxies()
            print(e)

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

    def on_disconnect(self, notice):
        print notice
        return True # Don't kill the stream