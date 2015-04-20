#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'

import time
import requests
from relay import *
from proxycheck import *
import multiprocessing as mp
from multiprocessing import Lock
from multiprocessing import Queue
from tweety.streaming import Stream

class Hydra():
    def __init__(self):
        self.threads = 5
        self.lifespan = 60
        self.processes = []
        self.version = '1.01'
        self.batchSize = 175
        self.proxyList = None
        self.streaming = False
        self.lock = masterLock
        self.tweetStack = Queue()
        self.auths = initAPIKeys()
        self.output = 'tweetDB.json'
        self.mode = 'morph' #['morph', 'geo']

    def run(self):
        print 'Running Hydra', self.version, 'in', self.mode, 'mode'
        self.main()

    def main(self):
        while True:
            self.tweetStack = Queue()
            try:
                if not self.proxyList:
                    self.refreshProxies()

                if self.mode == 'morph':
                    from meta import metadata
                    self.meta = metadata
                    self.processes = [mp.Process(target=openStream, args=(self.auths[x], self.mode, self.meta[x]['track'],
                                                                          self.meta[x]['pID'], self.proxyList[x],
                                                                          self.batchSize, self)) for x in range(self.threads)]
                if self.mode == 'geo':
                    from meta import geodata
                    self.meta = geodata
                    self.processes = [mp.Process(target=openStream, args=(self.auths[x], self.mode, self.meta[x]['crds'],
                                                                          self.meta[x]['pID'], self.proxyList[x],
                                                                          self.batchSize, self)) for x in range(self.threads)]

                self.printMapping()
                self.initiateStreaming()
                time.sleep(self.lifespan)
                self.terminateStreaming()

            finally:
                self.executeBatch(self.output)
                print 'Reconnecting...'


    def initiateStreaming(self):
        print 'Listening to', self.threads, 'data streams...'
        if self.streaming is False:
            for p in self.processes:
                p.start()
            self.streaming = True

    def terminateStreaming(self):
        if self.streaming is True:
            for p in self.processes:
                p.terminate()
            self.streaming = False

    def printMapping(self):
        print ''
        print '--------------Proxy Map----------------'
        for i in range(self.threads):
            print self.meta[i]['pID'], 'proxied to', self.proxyList[i]['http']
        print ''

    def executeBatch(self, json_filename):
        self.lock.acquire()

        buffer = []
        try:
            while not self.tweetStack.empty():
                tweet = self.tweetStack.get(timeout = 3)
                buffer.append(tweet)
            u = len(buffer)

            if u:
                try:
                    json_file = open(json_filename,'r')
                    json_data = json.load(json_file)
                    json_file.close()
                except:
                    json_data = []

                for tweet in buffer:
                    json_data.append({'user': tweet.userID, 'tweet': tweet.tweetID, 'text': tweet.text,
                                      'created_at': tweet.createdAt, 'location': tweet.location})

                json_file = open(json_filename,'w')
                json_file.write(json.dumps(json_data, cls=DateTimeEncoder))
                json_file.close()

                print 'Successfully wrote', u, 'entities to', json_filename

        except Exception as e:
            print e, 'exception caught while writing to database'

        finally:
            self.lock.release()

    def refreshProxies(self):
        self.lock.acquire()
        self.proxyList = fetchProxies(self.threads)
        self.lock.release()

    def handleNewTweet(self, pID, tweet):
        self.lock.acquire()
        try:
            self.tweetStack.put(tweet)
            print pID, ':    ', tweet.text
            print ''
        finally:
            self.lock.release()

    def processGeodata(self, tweet):
        if tweet.geodata:
            geodata = tweet.geodata['coordinates']
            tweet.trueLocation = {'lat':geodata[0], 'lon':geodata[1]}
        elif len(tweet.location):
            apiResponse = getLocationCoordinates(tweet.location)
            tweet.trueLocation['lat'] = apiResponse['lat']
            tweet.trueLocation['lon'] = apiResponse['lng']
            tweet.trueLocation['text'] = apiResponse['formatted_address']
        else:
            tweet.trueLocation = None
        print tweet.trueLocation

def openStream(auth ,mode, filter, pID, proxy, batchSize, streamHandler):
    sapi = Stream(auth, CustomStreamListener(streamHandler, pID, batchSize), pID, proxy)

    if mode == 'morph':
        sapi.filter(track=filter)
    elif mode == 'geo':
        sapi.filter(locations=filter)

def initAPIKeys():
    auths = []

    for i in range(5):
        auth = tweepy.OAuthHandler(CONSUMER_KEY[i], CONSUMER_SECRET[i])
        auth.set_access_token(OAUTH_TOKEN[i], OAUTH_TOKEN_SECRET[i])
        auths.append(auth)
    return auths

def getLocationCoordinates(location):
    response = requests.get('https://maps.googleapis.com/maps/api/geocode/json',
                            params={'address' : location, 'key' : 'AIzaSyCtaVbVYJrHPdbkj_gpxQWktZ-_5sJRyVk'})
    responseJSON = response.json()

    if responseJSON['status'] == 'OK':
        result = responseJSON['results'][0]
        return {'lat':result['geometry']['location']['lat'],
                'lng':result['geometry']['location']['lng'],
                'formatted_address':result['formatted_address']}
    else:
        return None

if __name__ == "__main__":
    masterLock = Lock()

    while True:
        try:
            hydra = Hydra()
            hydra.run()
        except Exception as e:
            print e, 'exception caught while running Hydra, reloading...'


