#! /usr/bin/env python
#encoding: utf-8

import time
import http.cookiejar, urllib
import json, lxml.html, feedparser

class HttpClientX:
	def __init__(self, proxy=None, encode='utf-8'):
        self.cookies = http.cookiejar.CookieJar()
        cookie_hdr = urllib.request.HTTPCookieProcessor(self.cookies)
        proxydic={} if proxy is None else {'http':proxy, 'https':proxy}
        proxy_hdr = urllib.request.ProxyHandler(proxydic)
        self.opener = urllib.request.build_opener(cookie_hdr, proxy_hdr)
        self.opener.addheaders = [('User-agent','Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1')]
        self.proxy=proxy
        self.encode=encode
        self.timeout=7
        self.delay=2
        self.tries=5
        self.cont=None

    def __get(self, url, fun, dat):
        t1=time.time()
        rst=None
        for i in range(self.tries):
            try:
                with self.opener.open(url, data=dat, timeout=self.timeout) as f: rst=fun(f)
                self.code = 200
                break
            except urllib.error.HTTPError as e:
                if i+1==self.tries:
                    print(self.proxy, 'failed fetching', url, e)
                    self.code = e.code
                    if e.code==404 or e.code == 403: break
                else: time.sleep(self.delay)
            except urllib.error.URLError as e:
                if i+1==self.tries:
                    print(self.proxy, 'failed fetching', url, e)
                    self.code = e.errno
                else: time.sleep(self.delay)
            except Exception as e:
                if i+1==self.tries:
                    print(self.proxy, 'failed fetching', url, e)
                    self.code = None
                else: time.sleep(self.delay)
        self.speed=time.time()-t1 if self.code == 200 else 1000
        return rst

    def __read(self, f):
        cont=''
        for l in f: cont+=l.decode(self.encode)
        return cont

    def get(self, url, ct='html', datDic=None):
        dat=None if datDic is None else urllib.parse.urlencode(datDic).encode('utf-8')
        cont = self.cont = self.__get(url, self.__read, dat)
        if cont is None: return None
        if ct == 'html': return cont
        elif ct == 'json': return json.loads(cont)
        elif ct == 'doc': return lxml.html.fromstring(cont[cont.find('<html'):])
        elif ct == 'feed': return self.__get(url, feedparser.parse, data)
        else: return cont

    def post(self, url, datdic, ct='html'):
        #tmp = {}
        #for k, v in datdic.items():
        #    tmp[k] = v if not isinstance(v, dict) else json.dumps(v)
        #dat=urllib.parse.urlencode(tmp).encode('utf-8')
        return self.get(url, ct, datdic)

    def save(self, fname):
        if self.cont is None: return
        with open(fname, 'w') as fw: fw.write(self.cont)

    def retrieve(self, url, fname):
        self.get(url)
        self.save(fname)

    def isOK(self):
        return self.code==200 or self.code==404

    def getCookieDict(self):
        return {cookie.name:cookie.value for cookie in self.cookies}


if __name__=='__main__':
    client=HttpClientX()
    url='https://foursquare.com/login'
    #url='http://www.douban.com'
    doc=client.get(url, 'doc')
    print(doc.find('.//title').text_content())
