#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'
from pyalchemy import alchemyapi
import requests
import json
import ast
import re

class Oracle():

    def __init__(self, keychain, Loki):
        print 'Invoking Oracle...'

        self.Loki = Loki
        self.Loki.decryptFile(keychain)

        #self.Loki.decryptFile('tweets.db')
        self.initResources()

    def initResources(self):

        from keys import GM_KEY
        self.gmkey = GM_KEY
        self.gmaps = 'https://maps.googleapis.com/maps/api/geocode/json'
        self.emo_db = json.load(open('pyalchemy/emoji_database','r'))
        self.alchemyAPI = alchemyapi.AlchemyAPI(self.Loki)
        self.count = 0

    def requestCoordinates(self, location):
        response = requests.get(self.gmaps, params={'address' : location, 'key' : self.gmkey})
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
                #print e, 'exception caught while parsing tweet'
                return 0
        else:
            return 0

    def processContents(self, myText):
        myText = self.resolveEmoji(myText)
        URLless_txt = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}     /)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', myText)
        Nickless_txt = ' '.join([word for word in URLless_txt.split() if not word.startswith('@')])
        return Nickless_txt


    def alchemyRequest(self, myText, mode):
        badStatus = ['daily-transaction-limit-exceeded', 'ERROR network-error', 'ERROR parse-error']
        self.count += 1
        result = 0
        if self.count > 1000:
            self.count = 0
            self.alchemyAPI.initResources()

        try:
            if mode is 'sentiment':
                response = self.alchemyAPI.sentiment('text', myText)
                if response['status'] == 'OK':
                    result = response['docSentiment']

            if mode is 'keywords':
                response = self.alchemyAPI.keywords('text', myText)
                if response['status'] == 'OK':
                    result = response

            if mode is 'combined':
                response = self.alchemyAPI.combined('text', myText)
                if response['status'] == 'OK':
                    result = response

            if response['status'] == 'ERROR':
                result = 'ERROR: '+ response['statusInfo']

            if response['statusInfo'] in badStatus:
                print 'Alchemy bad status, reloading'
                self.alchemyAPI.initResources(self.Loki)

        except Exception as e:
            pass

        finally:
            return result

    def resolveEmoji(self, myText):
        emostr = []
        emo_db = self.emo_db
        b = myText.encode('unicode_escape').split('\\')
        c = [point.replace('000','+').upper() for point in b if len(point) > 8 and point[0] == 'U']
        [emostr.append(emo_db[emo[:7]]) for emo in c if emo[:7] in emo_db]
        return myText + ' ' +' '.join(emostr)




