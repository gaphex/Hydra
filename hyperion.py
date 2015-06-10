__author__ = 'denisantyukhov'

import re
import sys
import json
import nltk
import time
import tweepy
import random
import urllib2
import Stemmer
import sqlite3
import operator
import pymorphy2
import numpy as np
import pandas as pd
from sys import stdout
from lshash import LSHash
from datetime import datetime
import matplotlib.pyplot as plt
from collections import Counter
from scipy.sparse import csr_matrix, vstack
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

class Hyperion():
    def __init__(self, data):
        print 'Invoking Hyperion...'
        self.morph = pymorphy2.MorphAnalyzer()
        self.stemmer = Stemmer.Stemmer('russian')
        self.data = data

    def prepare(self):
        con = sqlite3.connect('tweetsSpring.db')
        self.data = pd.read_sql("SELECT * from tweets",con)
        self.backup = self.preprocess()

    def processContents(self, myText):
        myText = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}     /)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', myText)
        words = [word for word in re.findall(r'(?u)[@|#]?\w+', myText) if not word.startswith(('@','#'))]
        words = self.stemmer.stemWords(words)
        return words

    def preprocess(self):

        print 'Preprocessing...this may take a while'
        t0 = time.time()
        terms = []
        words = []

        n = len(data.index)

        for i in range(n):
            terms.append(self.processContents(self.data.content_lower[i]))

        self.data['terms'] = terms[:]

        for i in range(n):
            words += terms[i]

        fdist = nltk.FreqDist(words)
        sortedDist = sorted(fdist.items(), key=operator.itemgetter(1),reverse=True)
        sortedDist = [x for x in sortedDist if len(x[0]) > 2]
        interestingVocab = [x[0] for x in sortedDist]

        #Find TF-IDF

        trainingList = []
        for i in range(n):
            trainingList.append(' '.join(data['terms'][i]))

        self.vectorizer = TfidfVectorizer(vocabulary = interestingVocab)
        self.tfidf = self.vectorizer.fit_transform(trainingList)  #finds the tfidf score with normalization

        print 'Building vocab and TF-IDF matrix took', time.time()-t0, 'seconds'
        print 'Vocab Length =', len(interestingVocab), '    Vector dimensionality =', n
        return(self.vectorizer, self.tfidf)


    def progress(self, i, n):
        stdout.write("\r%f%%" % (i*100/float(n)))
        stdout.flush()
        if i == n-1:
            stdout.write("\r100%")
            print("\r\n")

    def median(self,lst):
        return np.median(np.array(lst))

    def extract_urls(self, lst):
        urls = []
        for i in lst:
            for j in i.split(' '):
                if j.startswith('http'):
                    urls.append(j)
        return urls

    def resolve_url(self, starturl):
        try:
            req = urllib2.Request(starturl)
            res = urllib2.urlopen(req, timeout = 2)
            finalurl = res.geturl()
            return finalurl
        except:
            pass

    def setup(self, vectorizer, tf_idf):
        self.vectorizer = vectorizer
        self.tfidf = tf_idf
        print 'Setup complete '


    def findSimilarTweets(self, queryTweet, threshold, maxNumber = 0, log = False):

        t0 = time.time()

        processedTweet = ' '.join(self.processContents(queryTweet))
        queryTweetRepresentation = self.vectorizer.transform([processedTweet])

        cosine_similarities = cosine_similarity(queryTweetRepresentation, self.tfidf)[0]
        totalMatchingTweets = len(cosine_similarities[cosine_similarities>threshold])

        if maxNumber:
            totalMatchingTweets = min(totalMatchingTweets, maxNumber)
        indices = cosine_similarities.argsort()[::-1][:totalMatchingTweets]
        elapsed_time = time.time() - t0
        if len(indices) > 25:
            print 'Query:', queryTweet
        return indices

    def findOut(self, cluster):
        lst = list(self.data['content'][cluster])

        redirects = []
        keywords = []

        urls = self.extract_urls(lst)
        for url in urls:
            redirects.append(self.resolve_url(url))

        corpus = ' '.join(lst).lower()
        corpus = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}     /)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', corpus)
        words = []
        tokenized = nltk.tokenize.word_tokenize(corpus)
        for word in tokenized:
            words.append(word)

        fdist = nltk.FreqDist(words)
        sortedDist = sorted(fdist.items(), key=operator.itemgetter(1),reverse=True)
        moreThan3 = [x for x in sortedDist if len(x[0]) > 3 and x[1] > 3]
        for i in moreThan3:
            keywords.append(i[0])

        return(keywords, redirects)

    def resolve(self, masterCluster):
        t0 = time.time()
        k, r = self.findOut(masterCluster)

        insta = []
        swarm = []

        print 'Found', len(r), 'anonymous links, investigating...'
        for i, u in enumerate(r):
            if u:
                if 'instagram' in u:
                    insta.append(u)
                if 'swarmapp' in u:
                    swarm.append(u)

        print 'Resolving took', time.time()-t0
        return ((k, insta, swarm))

    def overlap(self, r1l, r1r, r1t, r1b, r2l, r2r, r2t, r2b):
        #Overlapping rectangles overlap both horizontally & vertically
        return self.range_overlap(r1l, r1r, r2l, r2r) and self.range_overlap(r1b, r1t, r2b, r2t)

    def range_overlap(self, a_min, a_max, b_min, b_max):
        #Neither range is completely greater than the other
        return (a_min <= b_max) and (b_min <= a_max)

    def assess(self, clusters):
        data = []
        for c in clusters:

            xs = [t[0] for t in c]
            ys = [t[1] for t in c]
            z = zip(xs, ys)
            n = len(z)

            dx = max(xs) - min(xs)
            dy = max(ys) - min(ys)

            mx = self.median(xs)
            my = self.median(ys)

            s=(dx+0.001)*(dy+0.001)
            score = n/s

            data.append((z,n,score,dx,dy,mx,my))
            data = filter(lambda a: a[1] > 5, data)

        return data

    def findUnique(self, stats):

        for v in range(3):
            for i in stats:
                r1l = i[5]-2*i[3]
                r1r = i[5]+2*i[3]
                r1b = i[6]-2*i[4]
                r1t = i[6]+2*i[4]
                for j in stats:
                    r2l = j[5]-2*j[3]
                    r2r = j[5]+2*j[3]
                    r2b = j[6]-2*j[4]
                    r2t = j[6]+2*j[4]
                    if self.overlap(r1l, r1r, r1t, r1b, r2l, r2r, r2t, r2b) and i!=j:
                        if i[1] >= j[1]:
                            try:
                                stats.remove(j)
                            except:
                                pass
                        else:
                            try:
                                stats.remove(i)
                            except:
                                pass

        return stats

    def performClusterisation(self, indices, thSp, thTm):

        #Spatial Clustering
        y = list(data['lat'][indices])
        x = list(data['long'][indices])
        tid = indices

        z = filter(lambda a: a[0] != (55.753301, 37.619899), zip(zip(x,y), tid))
        x = []
        y = []
        tid = []

        for t in z:
            x.append(t[0][0])
            y.append(t[0][1])
            tid.append(t[1])

        crds = zip(x,y)
        spUniques = self.findCluster(crds, tid, 34, thSp, thTm, 'space')
        if len(spUniques):
            print '--------------------------------------------------------------'
            print 'Found', len(spUniques), 'spatial clusters with a total of', sum([len(t[0]) for t in spUniques]), 'points among', len(indices)

        for w in spUniques:
            print len(w[0]), 'points at', w[3], w[4]


        #Temporal Clustering
        tmvc = []
        for t in data['created_at'][indices]:
            l = t.split(' ')
            date = l[0].split('-')
            time = l[1].split(':')
            datehash = int(date[1]) * 30 + int(date[2])
            timehash = int(time[0]) * 3600 + int(time[1]) * 60 + int(time[2])
            tmvc.append((datehash*400, timehash))

        tpUniques = self.findCluster(tmvc, tid, 34, thSp, thTm, 'time')
        dm = len(tpUniques)

        if dm:
            print '--------------------------------------------------------------'
            print 'Found', dm, 'temporal clusters with a total of', sum([len(t[0]) for t in tpUniques]), 'points among', len(indices)


        for w in tpUniques:
            date = w[3]/400
            time = w[4]
            print len(w[0]), 'points at', int(date//30),'.',int(date%30),'--',int(time//3600),
            print ':',int(time%3600//60),':',int(time%3600%60)

        print ''
        return ((spUniques, tpUniques))

    def findCluster(self, vecs, tids, cln, thSp, thTm, mode):

        maxn = 0
        maxc = []
        dim = len(vecs[0])
        data = zip(vecs, tids)


        if mode == 'space':
            metric = "euclidean"
            th = thSp
            rat = 1
        if mode == 'time':
            metric = 'hamming'
            th = thTm
            rat = 1

        lsh = LSHash(cln, dim)
        for i in vecs:
            lsh.index(i)

        pivots = list(set(random.sample(vecs, int(rat*len(vecs)))))

        for i in pivots:
            mxc = []
            dists = []
            u = lsh.query(i, distance_func=metric)

            for j in u:
                dists.append(j[1])
                mxc.append(j[0])

            r = self.cover(dists, th)
            maxc.append(mxc[0:r])


        scr = self.assess(maxc)
        rez = self.findUnique(scr)

        clusters = []
        for u in rez:
            cIDs = []
            for t in u[0]:
                for d in data:
                    if t == d[0]:
                        cIDs.append(d[1])
            clusters.append((cIDs, u[3], u[4], u[5], u[6]))

        clusters = filter(lambda a: len(a[0]) > 6, clusters)

        return (clusters)

    def cover(self, dat, th):
        sum = 0
        t = 0
        while sum < th*len(dat) and t < 0.5*len(dat):
            sum += dat[t]
            t += 1
        return t

    def doQuery(self, query, th_NN, th_SP, th_TM):
        t0 = time.time()
        indices = self.findSimilarTweets(query, th_NN)
        print self.data['content'][indices]
        print len(indices), 'points passed preprocessing'

        if len(indices):
            t = self.performClusterisation(indices, th_SP, th_TM)
            print "Query processing took", time.time()-t0, 'seconds'

        return t

def setupHyperion(data):
    hyperion = Hyperion(data)
    r = hyperion.preprocess()
    return hyperion, r
