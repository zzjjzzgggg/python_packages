#! /usr/bin/env python
#encoding: utf-8

from datetime import datetime
import os, time, json, bz2, gzip


class FileReader:
	def __init__(self, fname):
		ext = os.path.splitext(fname)[1]
		if ext == '.dat': 
			self.fr=open(fname)
			self.readline=self.__readline
		elif ext == '.bz2':
			self.fr=bz2.BZ2File(fname)
			self.readline=self.__readline2
		elif ext =='.gz':
			self.fr=gzip.open(fname)
			self.readline=self.__readline2
		else: 
			self.fr=open(fname)
			self.readline=self.__readline
	
	def __readline(self):
		return self.fr.readline().strip()
	
	def __readline2(self):
		return self.fr.readline().decode('utf8')

	def close(self):
		self.fr.close()

class FileIO:
	def __init__(self, fname, sep='\t'):
		print('Loading file', fname)
		self.sep=sep
		self.ln=0
		self.fr=FileReader(fname)
	
	def next(self):
		while True:
			self.l=self.fr.readline()
			if not self.l:
				self.fr.close()
				print('Totally', self.ln, 'lines.')
				return False
			if self.l[0]!='#':
				self.ln+=1
				self.items=self.l.strip().split(self.sep)
				return True

	def getLine(self):
		return self.l
	
	def getItemsCnt(self):
		return len(self.items)

	def get(self, i):
		return self.items[i]

	def getStr(self, i):
		return self.items[i]

	def getStrs(self):
		return self.items
	
	def getInt(self, i):
		return int(self.items[i])
	
	def getInts(self):
		return list(map(int, self.items))
	
	def getFlt(self, i):
		return float(self.items[i])
	
	def getFlts(self):
		return map(float, self.items)

	def getShortTime(self, i):
		return datetime.strptime(self.items[i], '%Y-%m-%d')

	def getTime(self, i):
		return datetime.strptime(self.items[i], '%Y-%m-%d %H:%M:%S')
	
	def fromTimestamp(self, i):
		return datetime.fromtimestamp(int(self.items[i]))

	def getISOTime(self, i):
		return datetime.strptime(self.items[i][:19], '%Y-%m-%dT%H:%M:%S')
	
	def getLN(self):
		return self.ln

class JsonStorer:
	def __init__(self, prefix, sufix='.json', data_dir='data/'):
		self.fw=None
		self.writed=0
		self.data_dir=data_dir
		if not os.path.exists(data_dir): os.mkdir(data_dir)
		self.prefix, self.sufix=prefix, sufix
	
	def __check(self):
		if self.writed>=50000 and self.fw is not None: self.close()
		if self.fw is None:
			self.fw=open(self.data_dir+self.prefix+str(time.time())+self.sufix, 'w')
			self.writed=0

	def save(self, item):
		self.__check()
		self.fw.write(json.dumps(item)+'\n')
		self.writed+=1
	
	def close(self):
		if self.fw is not None: self.fw.close()
		self.fw=None
		self.writed=0
		print('Storer closed!')

def saveList(slist, fnm, anno=None):
	fw=open(fnm, 'w')
	fw.write('#file: '+fnm+'\n')
	if anno is not None: fw.write(anno.strip()+'\n')
	for e in slist: fw.write(str(e)+'\n')
	fw.close()

def saveLists(slist, fnm, anno=None):
	fw=open(fnm, 'w')
	fw.write('# file: '+fnm+'\n')
	if anno is not None: fw.write('# '+anno.strip()+'\n')
	for e in slist: fw.write('\t'.join(map(str, e))+'\n')
	fw.close()

def saveMap(tmap, fnm, anno=None):
	fw=open(fnm, 'w')
	fw.write('#file: '+fnm+'\n')
	if anno is not None: fw.write(anno.strip()+'\n')
	for k,v in sorted(tmap.items()): fw.write('%s\t%s\n' % (str(k), str(v)))
	fw.close()

def saveSet(sset, fnm, anno=None):
	fw=open(fnm, 'w')
	fw.write('#file: '+fnm+'\n')
	if anno is not None: fw.write(anno.strip()+'\n')
	for e in sset: fw.write(str(e)+'\n')
	fw.close()

def saveTuples(dat, fnm, anno=None):
	fw=open(fnm, 'w')
	fw.write('#file: '+fnm+'\n')
	if anno is not None: fw.write(anno.strip()+'\n')
	for e in dat: fw.write('\t'.join(map(str, e))+'\n')
	fw.close()

def loadList(fname, col=0):
	rst=[]
	fio=FileIO(fname)
	while fio.next(): rst.append(fio.getStr(col))
	return rst

def loadIntList(fname, col=0):
	rst=[]
	fio=FileIO(fname)
	while fio.next(): rst.append(fio.getInt(col))
	return rst

def loadFltList(fname, col=0):
	rst=[]
	fio=FileIO(fname)
	while fio.next(): rst.append(fio.getFlt(col))
	return rst

def loadIntMap(fnm, ckey=0, cval=1):
	rst={}
	fio=FileIO(fnm)
	while fio.next(): rst[fio.getInt(ckey)]=fio.getInt(cval)
	return rst

def loadStrIntMap(fnm, ckey=0, cval=1):
	rst={}
	fio=FileIO(fnm)
	while fio.next(): rst[fio.getStr(ckey)]=fio.getInt(cval)
	return rst

def loadIntStrMap(fnm, ckey=0, cval=1):
	rst={}
	fio=FileIO(fnm)
	while fio.next(): rst[fio.getInt(ckey)]=fio.getStr(cval)
	return rst

def loadIntFltMap(fnm, ckey=0, cval=1):
	rst={}
	fio=FileIO(fnm)
	while fio.next(): rst[fio.getInt(ckey)]=fio.getFlt(cval)
	return rst

def loadStrFltMap(fnm, ckey=0, cval=1):
	rst={}
	fio=FileIO(fnm)
	while fio.next(): rst[fio.getStr(ckey)]=fio.getFlt(cval)
	return rst

def loadSet(fnm, c=0):
	rst=set()
	fio=FileIO(fnm)
	while fio.next(): rst.add(fio.getStr(c))
	return rst

def loadIntSet(fnm, c=0):
	rst=set()
	fio=FileIO(fnm)
	while fio.next(): rst.add(fio.getInt(c))
	return rst

def loadFltSet(fnm, c=0):
	rst=set()
	fio=FileIO(fnm)
	while fio.next(): rst.add(fio.getFlt(c))
	return rst

def loadIntPrList(fnm):
	rst=[]
	fio=FileIO(fnm)
	while fio.next(): rst.append((fio.getInt(0), fio.getInt(1)))
	return rst

def loadIntFltPrMap(fnm):
	rst={}
	fio=FileIO(fnm)
	while fio.next(): rst[fio.getInt(0)] = (fio.getFlt(1), fio.getFlt(2))
	return rst

def writeFile(fnm, data):
	if data is None or len(data)==0: return
	with open(fnm, 'w') as fw: fw.write(data)

