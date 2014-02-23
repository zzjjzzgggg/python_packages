#! /usr/bin/env python
#coding=utf-8

import pymongo
import random

class ProxyDAO(object):

	def __init__(self, host='192.168.4.238', port=27037, database='proxy'):
		self.__client=pymongo.MongoClient(host, port)
		self.db = self.__client[database]

	def saveHttpProxies(self, proxies):
		self.db.http.remove()
		for i, proxy in enumerate(proxies): self.db.http.save({'_id':i+1, 'proxy':proxy[0], 'delay':proxy[1]})

	def saveHttpsProxies(self, proxies):
		self.db.https.remove()
		for i, proxy in enumerate(proxies): self.db.https.save({'_id':i+1, 'proxy':proxy[0], 'delay':proxy[1]})
	
	def getHttpProxy(self, pid):
		item = self.db.http.find_one({'_id':pid})
		return item['proxy'] if item else None
	
	def getHttpProxyRandom(self, id_range):
		return None if not id_range else self.getHttpProxy(random.randint(id_range[0], id_range[1]))

	def getHttpsProxy(self, pid):
		item = self.db.https.find_one({'_id':pid})
		return item['proxy'] if item else None

	def getHttpsProxyRandom(self, id_range):
		return None if not id_range else self.getHttpsProxy(random.randint(id_range[0], id_range[1]))

	def count(self):
		return self.db.proxy.count()

	def getAll(self):
		rst=set()
		for proxy in self.db.http.find(): rst.add(proxy['proxy'])
		for proxy in self.db.https.find(): rst.add(proxy['proxy'])
		return rst

if __name__=='__main__':
	dao=ProxyDAO()
	print(dao.count())
	print(dao.getHttpProxyRandom(None))
	print(dao.getHttpsProxyRandom((1,3)))
