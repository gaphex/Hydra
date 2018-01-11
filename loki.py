__author__ = 'denisantyukhov'



import base64
from blowfish import Blowfish
from subprocess import Popen, PIPE
from utils import decorate, progress
from tqdm import tqdm
import time
import requests
import getpass
import urllib2
import sys
import os
import re

from config import salt, ip_url


class Loki():

    def __init__(self):
        print 'Invoking Loki...'

        if 'self.key' not in locals():
            self.authorise()

        self.delimiter = salt

    def authorise(self):
        while True:
            print 'Enter Password: '
            pwd = getpass.getpass()
            if len(pwd) > 7:
                self.key = pwd
                break
            else:
                print 'ACCESS DENIED'

    def encryptFile(self, input_f):
        try:
            key = self.key
            delimiter = self.delimiter
            size=int(self.GetSize(input_f))
            crypt_f=input_f+'.crypt'
            cipher = Blowfish(key)
            print ''
            
            decorate(' Encrypting ' + input_f + '...', 64, '-') 
            with open(input_f,'rb') as f1:
                with open(crypt_f,'wb') as f2:
                    for i in tqdm(range(size)):
                        t= f1.read(1)
                        u = cipher.encrypt(str(base64.b64encode(t)*2))+delimiter
                        f2.write(u)

            f1.close()
            f2.close()
            self.cleanUp(input_f)

        except Exception as e:
            print e, 'exception caught while encrypting', input_f

        finally:
            decorate('Success', 64, '-')

    def retryDecrypting(self, output_f):
        self.authorise()
        self.decryptFile(output_f)

    def decryptFile(self, crypt_f):
        key = self.key
        delimiter = self.delimiter
        c_file = crypt_f + '.crypt'
        if os.path.isfile(c_file):
            try:
                cipher = Blowfish(key)
                
                decorate(' Decrypting ' + c_file + '...', 64, '-')
                with open(c_file, 'rb') as f1:
                    with open(crypt_f, 'wb') as f2:
                        dt = f1.read().split(delimiter)
                        tot = len(dt)-1
                        for i in tqdm(range(tot)):

                            f2.write((base64.b64decode(cipher.decrypt(dt[i])[4:])))
                f1.close()
                f2.close()

                decorate('Success', 64, '-')

            except Exception as e:
                if str(e) == 'Incorrect padding':
                    print 'ACCESS DENIED'
                    self.retryDecrypting(crypt_f)
                else:
                    print e, 'exception caught while decrypting', crypt_f
        elif os.path.isfile(crypt_f):
            print 'encrypting keychain with a new key...'
            new_pass = raw_input('enter new pass --->>')
            if isinstance(new_pass, str):
                try:
                    self.key = new_pass
                    self.encryptFile(crypt_f)
                    self.retryDecrypting(crypt_f)
                except Exception as e:
                    print e



    def GetSize(self, input):
        f=open(input,'rb')
        f.seek(0,2) # move the cursor to the end of the file
        size = f.tell()
        f.close()
        return size

    @staticmethod
    def fetchProxies(n):
        
        print ('Fetching proxies...')
        pas = []
        while len(pas) < n:
            pas, fal = [], []
            myProxyList = get_proxies()
            print 'fetched', len(myProxyList), 'proxies, validating'
            bar = tqdm(total=n)
            for i in myProxyList:
                
                if len(pas) < n:
                    pas.append(i)
                    bar.update(1)
                    """
                    t = checkOut(i)
                    if t:
                        pas.append(t)
                        
                    else:
                        fal.append(t)
                    """
                else: break
        return pas

    def cleanUp(self, input_f):
        print 'Cleaning up...'
        for f in [input_f, input_f + 'c']:
            try:
                os.remove(f)
                print f, 'deleted'

            except Exception as e:
                pass


def get_proxies():
    proc = Popen(
        '''python2 fetch.py --anonymity "anonymous|elite" --threads 16 --type "http"''',
        shell=True,
        stdout=PIPE, stderr=PIPE
    )
    proc.wait()
    res = proc.communicate()
    urls = [{'http': t.strip().split()[0].split("//")[1]} 
            for t in res[0].split("\n") if 'http' in t]
    return urls


def checkOut(ip):
    try:
        proxy_handler = urllib2.ProxyHandler({'http': ip['http']})
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib2.install_opener(opener)
        req=urllib2.Request(ip_url)
        urllib2.urlopen(req, timeout=5)
        return ip
    except Exception as e:
        return None

