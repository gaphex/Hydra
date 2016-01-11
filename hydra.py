#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'

import time
import tweepy
import multiprocessing as mp
from multiprocessing import Lock



class Hydra():

    def __init__(self, nP, masterLock, mode, db, keychain, loki):


        self.db = db
        self.mode = mode
        self.loki = loki
        self.threads = nP
        self.lifespan = 60
        self.processes = []
        self.version = '1.05'
        self.proxyList = None
        self.streaming = False
        self.processed = False
        self.lock = masterLock
        self.keychain = keychain
        self.batchesProcessed = 0
        self.auths = self.initAPIKeys(nP)

    def run(self):
        print 'Running Hydra', self.version, 'in', self.mode, 'mode'
        self.cerberus = Cerberus(self.threads, masterLock, self.db, self.keychain, self.loki)
        self.main()

    def main(self):

            try:
                self.refreshProxies()
                print 'Logging on...'

                if self.mode == 'morph':
                    from meta import metadata
                    self.meta = metadata
                    self.processes = [mp.Process(target=self.openStream, args=(self.auths[x], self.mode, self.meta[x]['track'],
                                                                          self.meta[x]['pID'], self.meta[x]['pDesc'].decode('utf-8'), self.proxyList[x],
                                                                          self.cerberus, self)) for x in range(self.threads)]
                if self.mode == 'geo':
                    from meta import geodata
                    self.meta = geodata
                    self.processes = [mp.Process(target=self.openStream, args=(self.auths[x], self.mode, self.meta[x]['crds'],
                                                                          self.meta[x]['pID'], self.meta[x]['pDesc'].decode('utf-8'), self.proxyList[x],
                                                                          self.cerberus, self)) for x in range(self.threads)]

                #self.printMapping()

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
        print '---------------Connecting to', self.threads, 'data streams---------------'
        print ''
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
            print self.meta[i]['pDesc'], 'proxied to', self.proxyList[i]['http']
        print ''

    def refreshProxies(self):
        self.lock.acquire()
        self.proxyList = self.loki.fetchProxies(self.threads)
        self.lock.release()

    def process(self):
        batchspan = int(self.lifespan)
        r = 240

        self.processed = False
        while True:
            try:
                time.sleep(batchspan)
                score = self.cerberus.executeBatch()

                if sum(score) == 0:
                    print 'Stream died, rebooting..'
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

    from tweety.streaming import Stream
    from cerberus import Cerberus
    from relay import CustomStreamListener
    from loki import Loki

    keychain = 'keys.py'
    masterLock = Lock()
    mode = 'geo'
    db = 'mongo'
    nP = 5

    Loki = Loki()

    while True:
        try:

            hydra = Hydra(nP, masterLock, mode, db, keychain, Loki)
            hydra.run()

        except Exception as e:
            print e, 'exception caught while running Hydra'
            time.sleep(3)













