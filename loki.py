__author__ = 'denisantyukhov'



import base64
from blowfish import Blowfish
import random
import requests
import urllib2
import sys
import os
import re

class Loki():

    def __init__(self):
        print 'Invoking Loki...'

        if 'self.key' not in locals():
            self.authorise()

        self.delimiter = 'XMD5A'

    def authorise(self):
        while True:
            pwd = raw_input('Enter Password: ')
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
            print '----------------------------------'
            print 'Encrypting', input_f, '...'
            with open(input_f,'rb') as f1:
                with open(crypt_f,'wb') as f2:
                    for i in range(size):
                        progress(i, size)
                        t= f1.read(1)
                        u = cipher.encrypt(str(base64.b64encode(t)*2))+delimiter
                        f2.write(u)

            f1.close()
            f2.close()
            self.cleanUp(input_f)

        except Exception as e:
            print e, 'exception caught while encrypting', input_f

        finally:
            print 'Success'
            print '----------------------------------'
            print ''

    def retryDecrypting(self, output_f):
        self.authorise()
        self.decryptFile(output_f)

    def decryptFile(self, crypt_f):
        key = self.key
        delimiter = self.delimiter

        try:
            cipher = Blowfish(key)
            output_f = crypt_f
            crypt_f = crypt_f + '.crypt'
            print '----------------------------------'
            print 'Decrypting', crypt_f, '...'
            with open(crypt_f,'rb') as f1:
                with open(output_f,'wb') as f2:
                    lencr=f1.read().split(delimiter)
                    tot = len(lencr)-1
                    for i in range(tot):
                        progress(i, tot)
                        f2.write((base64.b64decode(cipher.decrypt(lencr[i])[4:])))
            f1.close()
            f2.close()
            print 'Success'
            print '----------------------------------'

        except Exception as e:

            if str(e) == "[Errno 2] No such file or directory: 'keys.py.crypt'":
                print 'Encrypting keychain with new key...'
            elif str(e) == 'Incorrect padding':
                print 'ACCESS DENIED'
                self.retryDecrypting(output_f)
            else:
                print e, 'exception caught while decrypting', crypt_f

    def GetSize(self, input):
        f=open(input,'rb')
        f.seek(0,2) # move the cursor to the end of the file
        size = f.tell()
        f.close()
        return size

    @staticmethod
    def fetchProxies(n):
        print ('----------------------------------')
        print ('Fetching proxies...')
        pas = []
        while len(pas) < n:
            pas, fal = [], []
            myProxyList = load_proxies('search-1304428#listable')
            print 'fetched', len(myProxyList), 'proxies, validating'
            for i in myProxyList:
                if len(pas) < n:
                    progress(len(pas), n, skip = 1)
                    t = checkOut(i)
                    if t:
                        pas.append(t)
                    else:
                        fal.append(t)
                else: break
        return pas

    def cleanUp(self, input_f):
        print 'Cleaning up...'
        try:
            os.remove(input_f)
            os.remove('keys.pyc')
            os.remove('loki.pyc')
            os.remove('meta.pyc')
            os.remove('relay.pyc')
            os.remove('oracle.pyc')
            os.remove('cerberus.pyc')
        except Exception as e:
            print e, 'exception caught while cleaning up'


def load_proxies(uri):
    p = scrape_hma(uri)
    ip_p = [ip.split('//')[1] for ip in p.split('\n') if len(ip.split('//'))==2]
    myProxyList = [{'http': v} for v in ip_p]
    return myProxyList


def scrape_hma(uri):
    r = requests.get('http://proxylist.hidemyass.com/'+uri)
    bad_class="("
    for line in r.text.splitlines():
        class_name = re.search(r'\.([a-zA-Z0-9_\-]{4})\{display:none\}', line)
        if class_name is not None:
           bad_class += class_name.group(1)+'|'

    bad_class = bad_class.rstrip('|')
    bad_class += ')'

    to_remove = '(<span class\="'+ bad_class + '">[0-9]{1,3}</span>|<span style=\"display:(none|inline)\">[0-9]{1,3}</span>|<div style="display:none">[0-9]{1,3}</div>|<span class="[a-zA-Z0-9_\-]{1,4}">|</?span>|<span style="display: inline">)'

    junk = re.compile(to_remove, flags=re.M)
    junk = junk.sub('', r.text)
    junk = junk.replace("\n", "")

    proxy_src = re.findall('([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})\s*</td>\s*<td>\s*([0-9]{2,6}).{100,1200}(socks4/5|HTTPS?)', junk)
    list = ''
    for src in proxy_src:
        if src[2] == 'socks4/5':
            proto = 'socks5h'
        else:
            proto = src[2].lower()
        if src:
            list += proto + '://' +src[0] + ':' + src[1] + '\n'
    return(list)


def checkOut(ip):
    try:
        proxy_handler = urllib2.ProxyHandler({'http': ip['http']})
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib2.install_opener(opener)
        req=urllib2.Request('http://www.icanhazip.com')
        urllib2.urlopen(req, timeout=5)
        return ip
    except Exception as e:
        return None


def progress(i, n, skip = 100, mode = 1):
    if i%skip == 0 and mode == 1:
        sys.stdout.write("\r%s%%" % "{:5.2f}".format(100*i/float(n)))
        sys.stdout.flush()
        if i >= (n/skip - 1)*skip:
            sys.stdout.write("\r100.00%")
            print("\r")
    if i%skip == 0 and mode == 2:
        sys.stdout.write("\r%s" % str(i))
        sys.stdout.flush()