#! /usr/bin/env python
#encoding: utf-8

import time
import urllib.request, urllib.error
import json, feedparser
import lxml.html

class HttpClient:
    def __init__(self, proxy=None, encode='utf8'):
        self.proxy=proxy
        self.encode=encode
        proxydic={} if proxy is None else {'http':proxy, 'https':proxy}
        proxy_handler = urllib.request.ProxyHandler(proxydic)
        self.opener = urllib.request.build_opener(proxy_handler)
        self.opener.addheaders = [('User-agent','Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1')]

    def get(self, url, tries=5):
        t1=time.time()
        dat=None
        for i in range(tries):
            try:
                dat=''
                with self.opener.open(url, timeout=10) as f:
                    for l in f: dat+=l.decode(self.encode, 'ignore')
                self.code = 200
                break
            except urllib.error.HTTPError as e:
                if i+1==tries:
                    print(self.proxy, 'failed fetching', url, e)
                    self.code = e.code
                    if e.code==404 or e.code ==403: break
                else: time.sleep(3)
            except urllib.error.URLError as e:
                if i+1==tries:
                    print(self.proxy, 'failed fetching', url, e)
                    self.code = e.errno
                else: time.sleep(3)
            except Exception as e:
                if i+1==tries:
                    print(self.proxy, 'failed fetching', url, e)
                    self.code = None
                else: time.sleep(3)
        self.speed=time.time()-t1 if self.code == 200 else 1000
        return dat

    def retrieve(self, url, fname):
        dat = self.get(url)
        if dat is None: return
        with open(fname, 'w') as fw: fw.write(dat)

    def getJson(self, url, tries=5):
        cont=self.get(url, tries)
        return json.loads(cont) if cont is not None else None

    def getFeed(self, url, tries=5):
        for i in range(tries):
            try:
                return feedparser.parse(url)
            except Exception as e:
                if i+1==tries: print(self.proxy, 'failed fetching', url, e)
                else: time.sleep(3)
        return None

    def getDoc(self, url, tries=5):
        html=self.get(url, tries)
        return lxml.html.document_fromstring(html) if html is not None else None

    def ping(self, url, kwd, proxy=None, tries=4):
        if proxy is not None: self.__init__(proxy)
        html=self.get(url, tries)
        suc = html is not None and html.find(kwd)>=0
        if proxy is not None: self.__init__(self.proxy)
        return suc

if __name__=='__main__':
    client=HttpClient()
    #url='http://www.56ads.com/data/rss/2.xml'
    url='http://www.douban.com'
    #dat=client.get(url)
    client.retrieve(url, 'test.pyc')
    print(client.code)
    #dat=feedparser.parse(dat)
    #dat=client.getJson('https://api.douban.com/shuo/v2/users/1000001/followers')
    #dat=client.getFeed('url')
    #dat=client.getDoc('http://www.xiami.com/space/lib-album/u/1431177')
    #print(dat.find('豆瓣'))
    #print(client.ping(url, '豆瓣'))
