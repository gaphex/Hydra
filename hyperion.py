# -*- coding: utf-8 -*-
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
import collections
import numpy as np
import pandas as pd
from sys import stdout
from lshash import LSHash
from datetime import datetime
import matplotlib.pyplot as plt
from collections import Counter
from sklearn.cluster import DBSCAN
from scipy.sparse import csr_matrix, vstack

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

class Hyperion():
    def __init__(self, data):
        print 'Invoking Hyperion...'
        self.stemmer = Stemmer.Stemmer('russian')
        self.probe = []
        self.data = data
        
    def processContents(self, myText):
        myText = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}     /)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', myText)
        words = [word for word in re.findall(r'(?u)[@|#]?\w+', myText) if not word.startswith(('@','#'))]
        words = self.stemmer.stemWords(words)
        return words
        
    def preprocess(self):
        
        print 'Preprocessing...this will take a while'
        t0 = time.time()
        terms = []
        words = []

        n = len(self.data.index)

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
            trainingList.append(' '.join(self.data['terms'][i]))

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

        processedTweet = ' '.join(self.processContents(queryTweet))
        queryTweetRepresentation = self.vectorizer.transform([processedTweet])

        cosine_similarities = cosine_similarity(queryTweetRepresentation, self.tfidf)[0]
        totalMatchingTweets = len(cosine_similarities[cosine_similarities>threshold])

        if maxNumber:
            totalMatchingTweets = min(totalMatchingTweets, maxNumber)
        indices = cosine_similarities.argsort()[::-1][:totalMatchingTweets]
        if len(indices) > 25:
            print 'Query:', queryTweet
            
        return indices
    
    def filter(self, x, y, tid):
        z = filter(lambda a: a[0] != (55.753301, 37.619899), zip(zip(x,y), tid))
        x = []
        y = []
        tid = []
        for t in z:
            x.append(t[0][0])
            y.append(t[0][1])
            tid.append(t[1])
        crds = zip(x,y)
        return crds, tid
    
    def performClusterisation(self, indices, thSp, thTm):
        
        #Spatial Clustering
        y = list(self.data['lat'][indices])
        x = list(self.data['long'][indices])
        tid = indices
        
        crds, tid = self.filter(x, y, tid)

        spUniques = self.findDBCluster(crds, tid, thSp, 5)
        
        if len(spUniques):
            print '--------------------------------------------------------------'
            print 'Found', len(spUniques), 'spatial clusters'

        for w in spUniques:
            kw = ' '.join(self.extractKeywords(w['ids'])[:5])
            print len(w['ids']), 'points at', w['xm'], w['ym'], kw


        #Temporal Clustering
        tmvc = []
        for t in self.data['created_at'][indices]:
            l = t.split(' ')
            date = l[0].split('-')
            time = l[1].split(':')
            datehash = int(date[1]) * 30 + int(date[2])
            timehash = int(time[0]) * 3600 + int(time[1]) * 60 + int(time[2])
            tmvc.append((datehash*400, timehash))

        tpUniques = self.findDBCluster(tmvc, tid, thTm, 5)

        if len(tpUniques):
            print '--------------------------------------------------------------'
            print 'Found', len(tpUniques), 'temporal clusters'
        

        for w in tpUniques:
            date = w['xm']/400
            time = w['ym']
            kw = ' '.join(self.extractKeywords(w['ids'])[:5])
            print len(w['ids']), 'points at', int(date//30),'.',int(date%30),'--',int(time//3600),
            print ':',int(time%3600//60),':',int(time%3600%60), kw

        print ''
        return ((spUniques, tpUniques))
    
    def extractKeywords(self, cluster):
        keywords = []
        corpus = ' '.join(list(self.data['content'][cluster]))
        corpus = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}     /)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', corpus)
        words = []
        tokenized = nltk.tokenize.word_tokenize(corpus)
        for word in tokenized:
            words.append(word)
        #words = self.stemmer.stemWords(words)

        fdist = nltk.FreqDist(words)
        sortedDist = sorted(fdist.items(), key=operator.itemgetter(1),reverse=True)
        moreThan3 = [x for x in sortedDist if len(x[0]) > 3 and x[1] > 3]
        for i in moreThan3:
            keywords.append(i[0])

        return keywords
    
    def processURLs(self, cluster):
        insta = []
        swarm = []
        redirects = []
        lst = list(self.data['content'][cluster])
        urls = self.extract_urls(lst)
        for i in urls:
            redirects.append(self.resolve_url(i))

        print 'Found', len(redirects), 'anonymous links, investigating...'
        for i, u in enumerate(redirects):
            if u:
                if 'instagram' in u:
                    insta.append(u)
                if 'swarmapp' in u:
                    swarm.append(u)

        return (insta, swarm)

    def findDBCluster(self, crds, tids, epsilon, min):

        clusters = []
        rez = []
        X = np.array(crds)
        db = DBSCAN(eps=epsilon, min_samples=min).fit(X)
        core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
        core_samples_mask[db.core_sample_indices_] = True
        labels = db.labels_
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
        unique_labels = set(labels)
        colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))
        for k, col in zip(unique_labels, colors):
            if k == -1:
                # Black used for noise.
                col = 'k'

            class_member_mask = (labels == k)

            xy = X[class_member_mask & core_samples_mask]
            #plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col, markeredgecolor='k', markersize=10)
            if len(xy):
                rez.append(xy)
            xy = X[class_member_mask & ~core_samples_mask]
            #plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col, markeredgecolor='k', markersize=2)

        #plt.title('Estimated number of clusters: %d' % n_clusters_)
        #plt.show()

        u = zip (crds, tids)
        for c in rez:
            buf = []
            for t in c:
                for j in u:
                    if tuple(t) == j[0]:
                        buf.append(j)
            clusters.append(list(set(buf)))

        scored = []
        for c in clusters:
            scored.append(self.score(c))

        scored = filter(lambda a: len(a['ids'])> min, scored)
        scored.sort(key = lambda t: t['score'], reverse = True)

        return scored
    
    def score(self, cluster):
        processed = {'ids':[], 'xm':0, 'ym':0, 'score':0, 'num':0}
        cluster = list(set(cluster))
        if len(cluster):
            xs = [t[0][0] for t in cluster]
            ys = [t[0][1] for t in cluster]
            ids = [t[1] for t in cluster]
            
            processed['ids'] = ids
            processed['xm'] = hyperion.median(xs)
            processed['ym'] = hyperion.median(ys)
            processed['score'] = len(ids)*len(ids)/((max(xs)-min(xs)+0.01)*(max(ys)-min(ys)+0.01))
            processed['num'] = len(ids)
            
        return processed

    def doQuery(self, query, th_NN, th_SP, th_TM):
        t0 = time.time()
        indices = self.findSimilarTweets(query, th_NN)
        print len(indices), 'points passed preprocessing'

        if len(indices):
            t = self.performClusterisation(indices, th_SP, th_TM)
            print "Query processing took", time.time()-t0, 'seconds'

        return t
            

def setupHyperion(data):
    hyperion = Hyperion(data)
    r = hyperion.preprocess()
    return hyperion, r
