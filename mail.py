__author__ = 'denisantyukhov'



import base64
from blowfish import Blowfish
from datetime import datetime
import email, random, imaplib
import zipfile
import urllib2
import sys
import os

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
                print 'Encrypting keychain with new key...',
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
        if not check_freshness():
            fetch_from_mail()
        pas, fal = [], []
        print ('----------------------------------')
        print ('Fetching proxies...')
        myProxyList = load_proxies()
        random.shuffle(myProxyList)
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

detach_dir = './assets/'
parch_dir = 'pr_arch/'
prox_dir = detach_dir + parch_dir


def load_proxies(m = 'f'):
    # m = raw_input("read from: f/i? -->>")
    if m == 'i':
        cmd = raw_input("insert proxies -->>")
        f = open(detach_dir + 'proxies','w')
        f.write(cmd)
        print ('wrote to file')
    if m == 'f':
        f = open(detach_dir + 'proxies', 'r')
        cmd = f.read()
    ip_p = list(set(cmd.split(' ')))
    myProxyList = [{'http': v} for v in ip_p]
    return myProxyList


def check_freshness():
    dat = '.'.join(str(datetime.now()).split(' ')[0].split('-')[1:])
    try:
        fn = [t for t in os.listdir(prox_dir) if t.startswith('proxylist')][-1]
        lst = '.'.join(fn.split('.')[0].split('-')[1:3])
    except:
        return 0

    if dat > lst:
        return 0
    else:
        return 1


def checkOut(ip):
    try:
        proxy_handler = urllib2.ProxyHandler({'http': ip['http']})
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib2.install_opener(opener)
        req=urllib2.Request('http://www.icanhazip.com')
        urllib2.urlopen(req, timeout=3)
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


def fetch_from_mail():
    m = login_to_email("imap.gmail.com")
    m.select('inbox')
    r,d = m.search("utf-8", "(SUBJECT %s)" % u"ProxyList".encode("utf-8"))
    items = d[0].split() # getting the mails id
    save_attachment(prox_dir, m, items[-1])

    fn = [t for t in os.listdir(prox_dir) if t.startswith('proxylist')][-1]
    with zipfile.ZipFile(prox_dir + fn, "r") as z:
        z.extractall(prox_dir)

    f = open(prox_dir + 'full_list/_reliable_list.txt', 'r')
    r = [{'http': ipp.split('\r\n')[0]} for ipp in f.readlines()]
    t = []
    for e, i in enumerate(r):
        progress(e, len(r), skip = 1)
        if checkOut(i):
            t.append(i['http'])

    f = open(detach_dir+'proxies', 'w')
    f.write(' '.join(t))
    return t


def login_to_email(imap):
    from keys import gm_lg, gm_ps
    user = gm_lg # raw_input("Enter your username:")
    pwd = gm_ps  # getpass.getpass("Enter your password: ")

    # connecting to the gmail imap server
    m = imaplib.IMAP4_SSL(imap)
    m.login(user,pwd)
    return m


def save_attachment(to_dir, m, emailid):
    resp, data = m.fetch(emailid, "(RFC822)") # fetching the mail, "`(RFC822)`" means "get the whole stuff", but you can ask for headers only, etc
    email_body = data[0][1] # getting the mail content
    mail = email.message_from_string(email_body) # parsing the mail content to get a mail object

    #Check if any attachments at all
    if mail.get_content_maintype() != 'multipart':
        return 0

    print "["+mail["From"]+"] :" + mail["Subject"]

    # we use walk to create a generator so we can iterate on the parts and forget about the recursive headach
    for part in mail.walk():
        # multipart are just containers, so we skip them
        if part.get_content_maintype() == 'multipart':
            continue

        # is this part an attachment ?
        if part.get('Content-Disposition') is None:
            continue

        filename = part.get_filename()
        counter = 1

        # if there is no filename, we create one with a counter to avoid duplicates
        if not filename:
            filename = 'part-%03d%s' % (counter, 'bin')
            counter += 1

        att_path = to_dir + filename
        # print att_path
        #Check if its already there
        if not os.path.isfile(att_path) :
            # finally write the stuff
            fp = open(att_path, 'wb')
            fp.write(part.get_payload(decode=True))
            fp.close()
