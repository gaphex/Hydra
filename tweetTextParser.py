#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'denisantyukhov'
import json
import re
import pymorphy2

class TweetTextParser():

    def __init__(self):
        print 'Invoking Processor...'
        self.morph = pymorphy2.MorphAnalyzer()
        try:
            self.emo_db = json.load(open('Hydra/pyalchemy/emoji_database','r'))
        except:
            print('No emoji database found')


    def processContents(self, myText, morphing):
        myText = self.resolveEmoji(myText)
        URLless_txt = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}     /)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', myText)
        usernameless_txt = ' '.join([word for word in URLless_txt.split() if not word.startswith('@')])
        if morphing:
            return ' '.join([self.morph.parse(word)[0].word for word in usernameless_txt.split()])
        else:
            return usernameless_txt

    def resolveEmoji(self, myText):
        emostr = []
        emo_db = self.emo_db
        b = myText.encode('unicode_escape').split('\\')
        c = [point.replace('000','+').upper() for point in b if len(point) > 8 and point[0] == 'U']
        [emostr.append(emo_db[emo[:7]]) for emo in c if emo[:7] in emo_db]
        return myText + ' ' +' '.join(emostr)
