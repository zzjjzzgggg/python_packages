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
		return (item['proxy'], item['delay']) if item is not None else (None,None)
	
	def getHttpProxyRandom(self, id_range=[]):
		cnt = self.db.http.count()
		if cnt == 0: return (None,None)
		if len(id_range)==0: id_range=[1,cnt]
		elif id_range[1]>cnt: id_range[1]=cnt
		return self.getHttpProxy(random.randint(id_range[0], id_range[1]))
	
	def getRndHttpSmart(self, nrequires=50):
		cnt = self.db.http.find({'delay':{'$lte':1}}).count()
		if cnt > nrequires: return self.getHttpProxyRandom([1,cnt])
		cnt = self.db.http.find({'delay':{'$lte':2}}).count()
		if cnt > nrequires: return self.getHttpProxyRandom([1,cnt])
		cnt = self.db.http.find({'delay':{'$lte':5}}).count()
		if cnt > nrequires: return self.getHttpProxyRandom([1,cnt])
		return self.getHttpProxyRandom()


	def getHttpsProxy(self, pid):
		item = self.db.https.find_one({'_id':pid})
		return item['proxy'] if item else None

	def getHttpsProxyRandom(self, id_range):
		cnt = self.db.https.count()
		if cnt == 0: return None
		if len(id_range)==0: id_range=[1,cnt]
		elif id_range[1]>cnt: id_range[1]=cnt
		return self.getHttpsProxy(random.randint(id_range[0], id_range[1]))

	def count(self):
		return self.db.proxy.count()

	def getAll(self):
		rst=set()
		for proxy in self.db.http.find(): rst.add(proxy['proxy'])
		for proxy in self.db.https.find(): rst.add(proxy['proxy'])
		return rst
	
	def info(self):
		nhttp = self.db.http.count()
		nhttps = self.db.https.count()
		print('total http proxies:', nhttp, 'total https proxies:', nhttps)
		nhttp = self.db.http.find({'delay':{'$lte':1}}).count()
		nhttps = self.db.https.find({'delay':{'$lte':1}}).count()
		print('http proxies with delay < 1 sec:', nhttp, 'https proxies with delay < 1 sec:', nhttps)
		nhttp = self.db.http.find({'delay':{'$lte':2}}).count()
		nhttps = self.db.https.find({'delay':{'$lte':2}}).count()
		print('http proxies with delay < 2 sec:', nhttp, 'https proxies with delay < 2 sec:', nhttps)
	
	def test(self):
		#for item in self.db.http.find():
		#	print('{:.4f}'.format(item['delay']))
		for i in range(100):
			proxy, delay = self.getRndHttpSmart(80)
			print(proxy, delay)
			print('{:.4f}'.format(delay))

if __name__=='__main__':
	dao=ProxyDAO()
	#print(dao.getHttpProxyRandom(None))
	#print(dao.getHttpsProxyRandom((1,3)))
	dao.info()
	#print(dao.getRndHttpSmart(10))
