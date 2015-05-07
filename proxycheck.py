__author__ = 'denisantyukhov'
import urllib2
from meta import *
import random

def checkOut(ip):
    try:
        proxy_handler = urllib2.ProxyHandler({'http': ip['http']})
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib2.install_opener(opener)
        req=urllib2.Request('http://www.icanhazip.com')
        urllib2.urlopen(req, timeout=3)
        print ip['http'], 'passed'
        print ''
        return ip

    except Exception as e:
        print ip['http'], 'failed:', e
        print ''
        return None



def fetchProxies(n):
    print '----------------------------------'
    print 'Fetching proxies...'
    responseList = []
    response = None
    test = None
    myProxyList = []
    while len(responseList) is not n:
        if not len(myProxyList):
            myProxyList = [{'http': v} for v in proxyList]
        else:
            random.shuffle(myProxyList)
            test = myProxyList.pop()
        if test:
            if checkOut(test):
                response = test
            else:
                response = None
        if response:
            if response not in responseList:
                responseList.append(response)
            else:
                print 'found dupe proxy:', response
    return responseList




