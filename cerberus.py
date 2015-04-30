__author__ = 'denisantyukhov'
from multiprocessing import Queue
from oracle import Oracle
import _sqlite3
import json


class dataHandler():
    def __init__(self, nP, masterLock, mode):
        self.nP = nP
        self.mode = mode
        self.totalScore = 0
        self.oracle = Oracle()
        self.lock = masterLock
        self.json_db_filename = 'tweetDB.json'
        self.SQL_db_filename = 'tweets.db'
        self.initResources()

    def initResources(self):
        self.stacks = []

        for i in range(self.nP):
            self.stacks.append(Queue())

        if self.mode == 'SQL':
            self.db = self.SQL_db_filename
            self.conn = _sqlite3.connect(self.SQL_db_filename)
            self.curs = self.conn.cursor()
            #self.curs.execute("CREATE TABLE tweets (tid integer, username text, cat text, content text, coordinates text, source text)")
        else:
            self.db = self.json_db_filename

    def handleNewTweet(self, pID, pDesc, tweet):
        self.lock.acquire()
        try:
            self.stacks[int(pID)].put(tweet)
            print pDesc, ':    ', tweet.text,
            r = self.oracle.howIsIt(tweet.text)
            if type(r) is dict:
                print r
            print ''

        finally:
            self.lock.release()

    def printScore(self, score, v):
        print ''
        print '     -----------------------------------------------------'
        print 'Successfully wrote', score, 'entities to', self.db, 'for a total of', v
        print '     -----------------------------------------------------'
        print ''

    def readFromJSON(self):
        json_file = open(self.json_db_filename,'r')
        json_data = json.load(json_file)
        json_file.close()
        return (json_data,len(json_data))

    def writeToJSON(self, json_data):
        json_file = open(self.json_db_filename,'w')
        json_file.write(json.dumps(json_data))
        json_file.close()
        return len(json_data)

    def executeBatch(self):
        self.lock.acquire()
        buffer = []
        try:
            for stack in self.stacks:

                while not stack.empty():
                    tweet = stack.get(timeout = 3)
                    buffer.append(tweet)

            if len(buffer):
                if self.mode == 'JSON':
                    json_data = []
                    try:
                        json_data, u = self.readFromJSON()
                    except Exception as e:
                        print e

                    for tweet in buffer:
                        json_data.append({'user': tweet.userID, 'tweet': tweet.tweetID, 'text': tweet.text,
                                          'created_at': str(tweet.createdAt), 'location': tweet.location})

                    self.writeToJSON(json_data)
                    score = len(buffer)
                    self.totalScore += score
                    self.printScore(score, self.totalScore)

                if self.mode == 'SQL':

                    for tweet in buffer:
                        self.curs.execute("insert into tweets (tid, username, content, cat, coordinates, source) values(?, ?, ?, ?, ?, ?)",
                                          (tweet.tweetID, tweet.userID, tweet.text, str(tweet.createdAt), tweet.location, tweet.device))
                        self.conn.commit()

                    score = len(buffer)
                    self.totalScore += score
                    self.printScore(score, self.totalScore)

        except Exception as e:
            print e, 'exception caught while writing to database'

        finally:
            self.lock.release()

    def reboot(self):
        self.executeBatch()
        self.initResources()
