#! /usr/bin/env python
#coding=utf-8

import random, time
import pymongo

class ProxyDAO(object):

	def __init__(self, host='192.168.4.238', port=27037, database='proxy'):
		self.__client=pymongo.MongoClient(host, port)
		self.db = self.__client[database]
		self.http_proxy_buffer = []
		self.http_lutm = 0
		self.https_proxy_buffer = []
		self.https_lutm = 0

	def saveHttpProxies(self, proxies):
		self.db.http.remove()
		for i, proxy in enumerate(proxies): self.db.http.save({'_id':i+1, 'proxy':proxy[0], 'delay':proxy[1]})

	def saveHttpsProxies(self, proxies):
		self.db.https.remove()
		for i, proxy in enumerate(proxies): self.db.https.save({'_id':i+1, 'proxy':proxy[0], 'delay':proxy[1]})
	
	def __update_http_buf(self, n):
		self.http_proxy_buffer = []
		for id in range(n):
			e = self.db.http.find_one({'_id':id+1})
			self.http_proxy_buffer.append((e['proxy'], e['delay']))
		self.http_lutm = time.time()
	
	def __update_https_buf(self, n):
		self.https_proxy_buffer = []
		for id in range(n):
			e = self.db.https.find_one({'_id':id+1})
			self.https_proxy_buffer.append((e['proxy'], e['delay']))
		self.https_lutm = time.time()
	
	def getRndHttpProxy(self, nrequires=50):
		if time.time() - self.http_lutm > 300:
			cnt = self.db.http.find({'delay':{'$lte':1}}).count()
			if cnt < nrequires: cnt = self.db.http.find({'delay':{'$lte':2}}).count()
			if cnt < nrequires: cnt = self.db.http.find({'delay':{'$lte':5}}).count()
			if cnt < nrequires: cnt = self.db.http.count()
			self.__update_http_buf(cnt)
		return random.choice(self.http_proxy_buffer) if len(self.http_proxy_buffer) > 0 else (None,None)

	def getRndHttpsProxy(self, nrequires=50):
		if time.time() - self.https_lutm > 300:
			cnt = self.db.https.find({'delay':{'$lte':1}}).count()
			if cnt < nrequires: cnt = self.db.https.find({'delay':{'$lte':2}}).count()
			if cnt < nrequires: cnt = self.db.https.find({'delay':{'$lte':5}}).count()
			if cnt < nrequires: cnt = self.db.https.count()
			self.__update_https_buf(cnt)
		return random.choice(self.https_proxy_buffer) if len(self.https_proxy_buffer) > 0 else (None,None)
	
	def getAllProxies(self):
		proxies = set()
		for e in self.db.http.find(): proxies.add(e['proxy'])
		for e in self.db.https.find(): proxies.add(e['proxy'])
		return proxies

	def info(self):
		nhttp = self.db.http.count()
		nhttps = self.db.https.count()
		print('total http proxies:',nhttp, 'total https proxies:',nhttps)
		nhttp = self.db.http.find({'delay':{'$lte':1}}).count()
		nhttps = self.db.https.find({'delay':{'$lte':1}}).count()
		print('http proxies with delay < 1 sec:', nhttp, 'https proxies with delay < 1 sec:', nhttps)
		nhttp = self.db.http.find({'delay':{'$lte':2}}).count()
		nhttps = self.db.https.find({'delay':{'$lte':2}}).count()
		print('http proxies with delay < 2 sec:', nhttp, 'https proxies with delay < 2 sec:', nhttps)

	
if __name__=='__main__':
	dao=ProxyDAO()
	dao.info()
	#for i in range(10): print(dao.getRndHttpsProxy(10))

