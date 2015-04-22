#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'

import time
from relay import *
from proxycheck import *
from supplement import *
import multiprocessing as mp
from multiprocessing import Lock
from multiprocessing import Queue
from tweety.streaming import Stream


class Hydra():
    def __init__(self):
        self.threads = nP
        self.lifespan = 600
        self.processes = []
        self.version = '1.02'
        self.proxyList = None
        self.streaming = False
        self.lock = masterLock
        self.auths = initAPIKeys()
        self.batchesPerStream = 50
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
                dataHandler.executeBatch()
                dataHandler.initQueues()
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



class dataHandler():
    def __init__(self):
        self.lock = masterLock
        self.db_filename = 'tweetDB.json'
        self.initQueues()

    def initQueues(self):
        self.stacks = []

        for i in range(nP):
            self.stacks.append(Queue())

    def handleNewTweet(self, pID, pDesc, tweet):
        self.lock.acquire()
        try:
            self.stacks[int(pID)].put(tweet)
            print pDesc, ':    ', tweet.text

        finally:
            self.lock.release()

    def printScore(self, score):
        print ''
        print '     ---------------------------------------'
        print 'Successfully wrote', score, 'entities to', self.db_filename
        print '     ---------------------------------------'
        print ''

    def readFrom(self, dbname):
        json_file = open(dbname,'r')
        json_data = json.load(json_file)
        json_file.close()
        return (json_data,len(json_data))

    def writeTo(self, dbname, json_data):
        json_file = open(self.db_filename,'w')
        json_file.write(json.dumps(json_data, cls=DateTimeEncoder))
        json_file.close()

    def executeBatch(self):
        self.lock.acquire()
        buffer = []
        json_data = []
        try:
            for stack in self.stacks:

                while not stack.empty():
                    tweet = stack.get(timeout = 3)
                    buffer.append(tweet)

            if len(buffer):
                try:
                    json_data, u = self.readFrom(self.db_filename)
                except Exception as e:
                    print e
                    pass

                for tweet in buffer:
                    json_data.append({'user': tweet.userID, 'tweet': tweet.tweetID, 'text': tweet.text,
                                      'created_at': tweet.createdAt, 'location': tweet.location})

                self.writeTo(self.db_filename, json_data)
                json_data, v = self.readFrom(self.db_filename)
                score = v - u
                self.printScore(score)


        except Exception as e:
            print e, 'exception caught while writing to database'

        finally:
            self.lock.release()



if __name__ == "__main__":
    masterLock = Lock()
    nP = 5
    mode = 'geo'    #['morph', 'geo']

    while True:
        try:
            dataHandler = dataHandler()
            hydra = Hydra()
            hydra.run()
        except Exception as e:
            print e, 'exception caught while running Hydra, reloading...'







