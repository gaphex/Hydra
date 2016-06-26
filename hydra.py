#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'


import sys
import time
import tweepy
import numpy as np
import multiprocessing as mp
from multiprocessing import Lock
from geopy.geocoders import Nominatim

from config import lifespan, op_mode, n_streams, db_type, key_file

from neutron.streaming import Stream
from cerberus import Cerberus
from relay import CustomStreamListener
from loki import Loki


class Hydra():

    def __init__(self, nP, masterLock, mode, db, keychain, loki, loc = None):


        self.db = db
        self.mode = mode
        self.loki = loki
        self.threads = nP
        self.lifespan = lifespan
        self.processes = []
        self.location = loc
        self.version = '1.07a'
        self.proxyList = None
        self.streaming = False
        self.processed = False
        self.lock = masterLock
        self.keychain = keychain
        self.batchesProcessed = 0
        
        self.auths = self.initAPIKeys(nP)
        self.loki.encryptFile(self.keychain)
        self.proxyList = self.loki.fetchProxies(self.threads)

    def run(self):
        print '\nRunning Hydra', self.version, 'in', self.mode, 'mode'
        self.cerberus = Cerberus(self.threads, masterLock, self.db, self.keychain, self.loki)
        self.main()

    def main(self):

            try:
                print 'Logging on...'

                if self.mode == 'morph':
                    from meta import metadata
                    self.meta = metadata
                    self.dbnames = [self.meta[x]['pDesc'].decode('utf-8') for x in range(self.threads)]
                    self.processes = [mp.Process(target=self.openStream, args=(self.auths[x], self.mode, self.meta[x]['track'],
                                                                               self.meta[x]['pID'],
                                                                               self.meta[x]['pDesc'].decode('utf-8'),
                                                                               self.proxyList[x],
                                                                               self.cerberus, self)) for x in range(self.threads)]

                elif self.mode == 'geo':
                    from meta import geodata
                    self.meta = geodata
                    self.dbnames = [self.meta[x]['pDesc'].decode('utf-8') for x in range(self.threads)]
                    self.processes = [mp.Process(target=self.openStream, args=(self.auths[x], self.mode, self.meta[x]['crds'],
                                                                               self.meta[x]['pID'],
                                                                               self.meta[x]['pDesc'].decode('utf-8'),
                                                                               self.proxyList[x],
                                                                               self.cerberus, self)) for x in range(self.threads)]
                    
                elif self.mode == 'loc':
                    name = []
                    C = []
                    for loc in self.location:
                        name.append(loc.address.split(',')[0])
                        d = 0.01
                        a,b = np.array(loc.point[0:2][::-1]) - np.array([d, d]), np.array(loc.point[0:2][::-1]) + np.array([d, d])
                        C.append(list(np.concatenate([a,b])))
                    self.dbnames = name
                    print name
                    self.processes = [mp.Process(target=self.openStream, args=(self.auths[x], 'geo', C[x],
                    x, name[x].decode('utf-8'), self.proxyList[x], self.cerberus, self)) for x in range(self.threads)]

                self.initiateStreaming()
                self.process()
                self.terminateStreaming()

            except Exception as e:
                print e, 'exception caught while listening to streams'

                #if not self.processed:
                    #self.Loki.encryptFile(self.keychain)
                    #self.Loki.encryptFile('tweets.db')


    def initiateStreaming(self):
        print ''
        if self.threads > 1: s = 's'
        else: s = ''
        print '---------------Connecting to', self.threads, 'data stream'+s+'---------------'
        print ''
        if self.streaming is False:
            for p in self.processes:
                p.start()
            self.streaming = True

    def terminateStreaming(self):
        if self.streaming is True:
            for p in self.processes:
                p.terminate()
            for p in self.processes:
                p.join()
            self.streaming = False

    def printMapping(self):
        print ''
        print '--------------Proxy Map----------------'
        for i in range(self.threads):
            print self.meta[i]['pDesc'], 'proxied to', self.proxyList[i]['http']
        print ''

    def process(self):
        batchspan = int(self.lifespan)
        r = 240

        self.processed = False
        while True:
            try:
                time.sleep(batchspan)
                score = self.cerberus.executeBatch(self.dbnames)
                dead = len([i for i in score if i == 0])
                if dead > 2:
                    print dead, 'streams have died, rebooting..'
                    break

                if self.batchesProcessed % r == 0 and self.batchesProcessed > 0:
                    print 'Memory cleanup, rebooting..'
                    break
                self.batchesProcessed += 1

            except Exception as e:
                print 'Exception', e, 'caught while listening, rebooting..'
                break

    def initAPIKeys(self, nP):
        from keys import CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET
        auths = []

        for i in range(nP):
            auth = tweepy.OAuthHandler(CONSUMER_KEY[i], CONSUMER_SECRET[i])
            auth.set_access_token(OAUTH_TOKEN[i], OAUTH_TOKEN_SECRET[i])
            auths.append(auth)
        return auths

    def openStream(self, auth ,mode, filter, pID, pDesc, proxy, dataHandler, streamHandler):
        sapi = Stream(auth, CustomStreamListener(dataHandler, streamHandler, pID, pDesc), pID, proxy)

        if mode == 'morph':
            sapi.filter(track=filter)
        elif mode == 'geo':
            sapi.filter(locations=filter)


if __name__ == "__main__":
    
    keychain = key_file
    masterLock = Lock()
    mode = op_mode
    db = db_type
    nP = n_streams

    
    locs = []
    if len(sys.argv) > 1:
        geolocator = Nominatim()
        for argv in sys.argv[1:]:
            location = geolocator.geocode(str(argv))
            if location:
                print location
                locs.append(location)
                mode = 'loc'
        nP = len(locs)

    Loki = Loki()


    while True:
        try:
            Loki.decryptFile(keychain)
            hydra = Hydra(nP, masterLock, mode, db, keychain, Loki, locs)
            hydra.run()
            
        except Exception as e:
            Loki.encryptFile(keychain)
            print e, 'exception caught while running Hydra'
            time.sleep(3)
