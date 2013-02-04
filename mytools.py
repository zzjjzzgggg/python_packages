#! /usr/bin/env python
#encoding: utf-8

import re

rex_num = re.compile('\d+')

def getNums(ss, ignore_comma=False):
	if ignore_comma: ss=ss.replace(',', '')
	return list(map(int, rex_num.findall(ss)))

def getFstNum(ss, ignore_comma=False):
	nums=getNums(ss, ignore_comma)
	return 0 if len(nums)==0 else nums[0]

def getLstNum(ss, ignore_comma=False):
	nums=getNums(ss, ignore_comma)
	return 0 if len(nums)==0 else nums[-1]

if __name__=='__main__':
	print(getNums('asdfsa_12,34_asdf_456', True))
