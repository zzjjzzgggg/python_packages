#! /usr/bin/env python
#encoding: utf-8

import re

class URLParser:
	def __init__(self):
		self.rex=re.compile('<a([^><]+)>')
	
	def getURL(self, astr):
		p1=astr.find('href="http://')
		if p1==-1: return None
		p1+=6
		p2=astr.find('"', p1)
		if p2==-1: return None
		return astr[p1:p2]

	def parse(self, wbstr):
		urls=set()
		for sub in self.rex.findall(wbstr):
			url=self.getURL(sub)
			if url is not None: urls.add(url)
		return urls

if __name__=='__main__':
	pass
