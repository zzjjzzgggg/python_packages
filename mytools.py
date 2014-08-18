#! /usr/bin/env python
#encoding: utf-8

import re
import time

rex_num = re.compile('\d+')
rex_money = re.compile('[$￥\d\.]+')
def getNums(ss, ignore_comma=False):
	if ignore_comma: ss=ss.replace(',', '')
	return list(map(int, rex_num.findall(ss)))

def getFstNum(ss, ignore_comma=False):
	nums=getNums(ss, ignore_comma)
	return 0 if len(nums)==0 else nums[0]

def getLstNum(ss, ignore_comma=False):
	nums=getNums(ss, ignore_comma)
	return 0 if len(nums)==0 else nums[-1]

def getMoney(ss):
	ms = rex_money.findall(ss)
	return None if len(ms)==0 else ms[0]

def timestamp(sec=True):
	if sec: return int(time.time())
	else: return int(time.time()*1000)

def F(x):
	''' Format x '''
	if x<1000: return str(x)
	elif x<1000000: return '{:d}K'.format(x//1000)
	elif x<1000000000: return '{:d}M'.format(x//1000000)
	else: return '{:d}B'.format(x//1000000000)

def prettytime(x):
	if x<60: return '{:.2f} secs'.format(x)
	elif x<3600: return '{:.2f} mins'.format(x/60)
	else: return '{:.2f} hrs'.format(x/3600)

class Timer:
	def __init__(self):
		self.tick()
	
	def tick(self):
		self.start = time.time()
		
	def tmstr(self):
		tme = time.time() - self.start
		return prettytime(tme)

if __name__=='__main__':
	#print(getNums('asdfsa_12,34_asdf_456aaa34.55'))
	#print(getMoney('asdf￥9.90asdfa'))
	#print(timestamp())
	print(prettytime(60876))

