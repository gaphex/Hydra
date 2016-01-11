__author__ = 'denisantyukhov'
from multiprocessing import Queue
from oracle import Oracle
from pymongo import MongoClient
from meta import geodata
import _sqlite3
import json


class Cerberus():
    def __init__(self, nP, masterLock, mode, keychain, Loki):
        self.nP = nP
        self.mode = mode
        self.totalScore = 0
        self.lock = masterLock
        self.oracle = Oracle(keychain, Loki)
        self.json_db_filename = 'tweetDB.json'
        self.SQL_db_filename = 'tweets.db'
        self.nbatches = 0
        self.initResources()

    def initResources(self):
        self.stacks = [Queue() for _ in range(self.nP)]
        if self.mode == 'mongo':
            self.client = MongoClient()
            self.db = self.client.tweets

        if self.mode == 'SQL':
            self.db = self.SQL_db_filename
            self.conn = _sqlite3.connect(self.SQL_db_filename)
            self.curs = self.conn.cursor()
            self.curs.execute("CREATE TABLE IF NOT EXISTS tweets (tid integer, username text, cat text, content text, coordinates text, sentiment text, source text)")

        if self.mode == 'JSON':
            self.db = self.json_db_filename

    def db_count(self, table):
        rowsQuery = "SELECT Count() FROM %s" % table
        self.curs.execute(rowsQuery)
        numberOfRows = self.curs.fetchone()[0]
        return numberOfRows

    def handleNewTweet(self, pID, pDesc, tweet):
        self.lock.acquire()
        #print tweet
        try:
            self.stacks[int(pID)].put(tweet)
        except Exception as e:
            print 'caught', e, 'while handling new entry'
        finally:
            self.lock.release()

    def printScore(self, batchc):
        if self.mode == 'mongo':
            name = self.mode + '.' + self.db.name
            dbc = sum([self.db[cname].count() for cname in self.db.collection_names()])
        else:
            name = self.db
            dbc = self.db_count('tweets')
        print ''
        print '--------------------------Batch #{:4d}--------------------------------------'.format(self.nbatches)
        print 'Successfully wrote', sum(batchc), 'entities to', name, 'for a total of', dbc
        print '---------------------{:12s}-------------------------'.format(str(batchc))
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
        self.nbatches += 1
        buffers = []
        batch_cnt = []
        try:
            for i, stack in enumerate(self.stacks):
                buf = []
                while not stack.empty():
                    tweet = stack.get(timeout = 3)
                    buf.append(tweet)
                buffers.append(buf)
                batch_cnt.append(len(buf))

            if len(buffers):
                if self.mode == 'JSON':
                    json_data = []
                    try:
                        json_data, u = self.readFromJSON()
                    except Exception as e:
                        print e, 'exception caught while writing to JSON'
                    for buf in buffers:
                        for tweet in buf:
                            json_data.append({'user': tweet.userID, 'tweet': tweet.tweetID, 'text': tweet.text,
                                              'created_at': str(tweet.createdAt), 'location': tweet.location, 'source': tweet.device})

                    self.writeToJSON(json_data)
                    score = len(buf)
                    self.totalScore += score
                    self.printScore(score, self.totalScore)

                if self.mode == 'SQL':
                    for buf in buffers:
                        for tweet in buf:
                            self.curs.execute("insert into tweets (tid, username, content, cat, coordinates, sentiment, source) values(?, ?, ?, ?, ?, ?, ?)",
                                              (tweet.tweetID, tweet.userID, tweet.text, str(tweet.createdAt), tweet.location, tweet.sentiment, tweet.device))
                            self.conn.commit()

                    self.printScore(batch_cnt)

                if self.mode == 'mongo':
                    score = 0
                    for i, buf in enumerate(buffers):
                        col = geodata[i]['pDesc']
                        for obj in buf:
                            tid = obj.json['id_str']
                            self.db[col].insert_one({'_id': tid, 'payload':obj.json})
                            #self.db[col].insert_many([doc.status._json for doc in buf])
                    self.printScore(batch_cnt)

        except Exception as e:
            print e, 'exception caught while writing to database'

        finally:
            self.lock.release()
            return batch_cnt

    def reboot(self):
        del self.stacks
        self.executeBatch()
        self.initResources()
