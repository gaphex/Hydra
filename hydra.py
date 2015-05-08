#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'
import multiprocessing as mp
from multiprocessing import Lock
from tweety.streaming import Stream
import tweepy
import time
from cerberus import dataHandler
from relay import CustomStreamListener
from loki import EncryptFile, fetchProxies



class Hydra():


    def __init__(self, nP, masterLock, mode):

        self.mode = mode
        self.threads = nP
        self.lifespan = 600
        self.processes = []
        self.version = '1.04'
        self.proxyList = None
        self.keychain = keychain
        self.key = key
        self.streaming = False
        self.lock = masterLock
        self.batchesPerStream = 20
        self.auths = self.initAPIKeys(nP)

    def run(self):
        print 'Running Hydra', self.version, 'in', self.mode, 'mode'

        self.main()

    def main(self):
        while True:
            try:
                if not self.proxyList:
                    self.refreshProxies()

                if self.mode == 'morph':
                    from meta import metadata
                    self.meta = metadata
                    self.processes = [mp.Process(target=self.openStream, args=(self.auths[x], self.mode, self.meta[x]['track'],
                                                                          self.meta[x]['pID'], self.meta[x]['pDesc'], self.proxyList[x],
                                                                          dataHandler, self)) for x in range(self.threads)]
                if self.mode == 'geo':
                    from meta import geodata
                    self.meta = geodata
                    self.processes = [mp.Process(target=self.openStream, args=(self.auths[x], self.mode, self.meta[x]['crds'],
                                                                          self.meta[x]['pID'], self.meta[x]['pDesc'], self.proxyList[x],
                                                                          dataHandler, self)) for x in range(self.threads)]

                #self.printMapping()
                self.initiateStreaming()
                self.process()
                self.terminateStreaming()

            except Exception as e:
                print e, 'exception caught while listening to streams'
            finally:
                dataHandler.reboot()
                EncryptFile(self.keychain, self.key)


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
            print self.meta[i]['pDesc'], 'proxied to', self.proxyList[i]['http']
        print ''

    def refreshProxies(self):
        self.lock.acquire()
        self.proxyList = fetchProxies(self.threads)
        self.lock.release()

    def process(self):
        batchspan = int(self.lifespan/self.batchesPerStream)

        for i in range (self.batchesPerStream):
            time.sleep(batchspan)
            dataHandler.executeBatch()

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

    masterLock = Lock()
    mode = 'morph'
    db = 'SQL'
    key = raw_input('Enter Password: ')
    keychain = 'keys.py'
    nP = 1

    while True:
        try:
            dataHandler = dataHandler(nP, masterLock, db, keychain, key)
            hydra = Hydra(nP, masterLock, mode)
            hydra.run()

        except KeyboardInterrupt:
            hydra.terminateStreaming()
            print '--------------Shutting Down----------------'
            break

        except Exception as e:
            print e, 'exception caught while running Hydra, reloading...'
            time.sleep(3)









