#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'

import time
from relay import *
from proxycheck import *
from oracle import *
import multiprocessing as mp
from cerberus import dataHandler
from multiprocessing import Lock
from tweety.streaming import Stream

class Hydra():
    def __init__(self, nP, masterLock, mode):
        self.threads = nP
        self.lifespan = 600
        self.processes = []
        self.version = '1.03'
        self.proxyList = None
        self.streaming = False
        self.lock = masterLock
        self.auths = initAPIKeys(nP)
        self.batchesPerStream = 30
        self.mode = mode

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

                self.printMapping()
                self.initiateStreaming()
                self.process()
                self.terminateStreaming()

            except Exception as e:
                print e, 'exception caught while listening to streams'
            finally:
                dataHandler.reboot()
                print ''
                print 'Reconnecting...'
                print ''


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

    def openStream(self, auth ,mode, filter, pID, pDesc, proxy, dataHandler, streamHandler):
        sapi = Stream(auth, CustomStreamListener(dataHandler, streamHandler, pID, pDesc), pID, proxy)

        if mode == 'morph':
            sapi.filter(track=filter)
        elif mode == 'geo':
            sapi.filter(locations=filter)


if __name__ == "__main__":

    masterLock = Lock()
    mode = 'geo'
    db = 'SQL'
    nP = 1

    while True:
        try:
            dataHandler = dataHandler(nP, masterLock, db)
            hydra = Hydra(nP, masterLock, mode)
            hydra.run()

        except Exception as e:
            if str(e) == 'Cannot operate on a closed database.':
                print ''
                print '--------------Shutting Down----------------'
                break
            else:
                print e, 'exception caught while running Hydra, reloading...'
                time.sleep(3)









