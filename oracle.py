#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'
import requests
import tweepy
from meta import *
import json
import ast
import re
from pyalchemy import alchemyapi



class Oracle():

    def __init__(self):
        self.key = 'AIzaSyCtaVbVYJrHPdbkj_gpxQWktZ-_5sJRyVk'
        self.gmaps = 'https://maps.googleapis.com/maps/api/geocode/json'
        self.alchemyAPI = alchemyapi.AlchemyAPI()
        self.emo_db = json.load(open('pyalchemy/emoji_database','r'))

    def requestCoordinates(self, location):
        response = requests.get(self.gmaps, params={'address' : location, 'key' : self.key})
        responseJSON = response.json()

        if responseJSON['status'] == 'OK':
            result = responseJSON['results'][0]
            return {'lat':result['geometry']['location']['lat'],
                    'lng':result['geometry']['location']['lng'],
                    'formatted_address':result['formatted_address']}
        else:
            return None

    def findInRaw(self, request, raw):
        u = raw.find(request)
        if u != -1:
            try:
                if request == 'bounding_box':
                    r1 = raw[u:u+200].split('{')[1].split('}')[0]
                    u = r1.find('[[[')
                    box = ast.literal_eval(r1[u+1:u+105])
                    return box
                else:
                    return raw[u:u+30].split(' ')[1].split("'")[1]
            except Exception as e:
                return 0
        else:
            return 0

    def howIsIt(self, myText):
        sc = self.resolveEmoji(myText)
        if sc:
            myText = myText + sc
        URLless_txt = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}     /)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', myText)
        Nickless_txt = ' '.join([word for word in URLless_txt.split() if not word.startswith('@')])
        response = self.alchemyAPI.sentiment('text', Nickless_txt)

        try:
            if response['statusInfo'] == 'daily-transaction-limit-exceeded':
                print '----------alchemy transaction limit reached----------'
                self.alchemyAPI.initResources()
            if response['status'] == 'OK':
                if 'score' in response['docSentiment']:
                    return response['docSentiment']['type'],response['docSentiment']['score']
            if response is None:
                print ('error')
                response = 0

        except Exception as e:
            pass

        finally:
            return response['docSentiment']

    def resolveEmoji(self, myText):
        emostr = []
        emo_db = self.emo_db
        b = myText.encode('unicode_escape').split('\\')
        c = [point.replace('000','+').upper() for point in b if len(point) > 8 and point[0] == 'U']
        [emostr.append(emo_db[emo[:7]]) for emo in c if emo[:7] in emo_db]
        return ' '.join(emostr)




