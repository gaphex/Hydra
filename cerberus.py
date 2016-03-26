__author__ = 'denisantyukhov'
from multiprocessing import Queue
from pymongo import MongoClient
from datetime import datetime
from oracle import Oracle
from meta import geodata
import _sqlite3
import json


class Cerberus(object):
    def __init__(self, n_streams, master_lock, mode, keychain, loki):
        self.nP = n_streams
        self.mode = mode
        self.totalScore = 0
        self.lock = master_lock
        self.oracle = Oracle(keychain, loki)
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
        r_query = "SELECT Count() FROM %s" % table
        self.curs.execute(r_query)
        n_rows = self.curs.fetchone()[0]
        return n_rows

    def handleNewTweet(self, pID, pDesc, tweet):
        self.lock.acquire()
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

        batch_number = 'Batch ' + str(self.nbatches)
        announcement = '    Wrote ' + str(sum(batchc)) + \
                       ' entities to ' + str(name) + ' for a total of ' + str(dbc) + '   '
        statistics = str(batchc)

        l = len(announcement)
        t1 = int((l - len(batch_number))/2)
        t2 = int((l - len(statistics))/2)

        print ''
        print t1*'-', batch_number, (l-len(batch_number)-t1)*'-'
        print announcement
        print t2*'-', statistics, (l-len(statistics)-t2)*'-'

    def readFromJSON(self):
        json_file = open(self.json_db_filename,'r')
        json_data = json.load(json_file)
        json_file.close()
        return json_data, len(json_data)

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
                            self.curs.execute("insert into tweets (tid, username, content, cat, /"
                                              "coordinates, sentiment, source) values(?, ?, ?, ?, ?, ?, ?)",
                                              (tweet.tweetID, tweet.userID, tweet.text, str(tweet.createdAt),
                                               tweet.location, tweet.sentiment, tweet.device))
                            self.conn.commit()

                    self.printScore(batch_cnt)

                if self.mode == 'mongo':
                    score = 0
                    for i, buf in enumerate(buffers):
                        col = geodata[i]['pDesc']
                        for obj in buf:
                            tid = obj.json['id_str']
			    cat = date_convert(obj.json['created_at'])
                            self.db[col].insert_one({'_id': tid, 'created': cat, 'payload':obj.json})

                    self.printScore(batch_cnt)

        except Exception as e:
            print type(e).__name__,'exception caught while writing to database'

        finally:
            self.lock.release()
            return batch_cnt

    def reboot(self):
        del self.stacks
        self.executeBatch()
        self.initResources()

def date_convert(strdate):
    return datetime.strptime(strdate, '%a %b %d %H:%M:%S +0000 %Y')
