#! /usr/bin/env python
#encoding: utf-8

from datetime import datetime
import os, sys, time, json, bz2, gzip

class FileReader:
	def __init__(self, fname):
		ext = os.path.splitext(fname)[1]
		if ext == '.dat': self.fr=open(fname)
		elif ext == '.bz2':	self.fr=bz2.open(fname, 'rt')
		elif ext =='.gz': self.fr=gzip.open(fname, 'rt')
		else: self.fr=open(fname)
	
	def readline(self):
		return self.fr.readline().strip()
	
	def readlines(self):
		return [l for l in self.fr]
	
	def close(self):
		self.fr.close()

class FileWriter:
	def __init__(self, fname):
		ext = os.path.splitext(fname)[1]
		if ext == '.bz2': self.f=bz2.open(fname, 'wt')
		elif ext =='.gz': self.f=gzip.open(fname, 'wt')
		else: self.f=open(fname, 'w')
	
	def __enter__(self):
		return self
	
	def __exit__(self,*exc):
		self.close()
		return False

	def write(self, l):
		self.f.write(l)
	
	def flush(self):
		self.f.flush()
	
	def close(self):
		self.f.close()

class FileIO:
	def __init__(self, fname, sep='\t', com='#', echo=True):
		if echo: print('Loading file', fname, end=' ... ')
		sys.stdout.flush()
		self.sep=sep
		self.com=com
		self.ln=0
		self.fr=FileReader(fname)
		self.echo=echo
	
	def __enter__(self):
		return self
	
	def __exit__(self,*exc):
		self.close()
		return False
	
	def close(self):
		self.fr.close()
		if self.echo: print('totally', self.ln, 'lines.')

	def next(self):
		while True:
			self.l=self.fr.readline()
			if not self.l:
				self.close()
				return False
			if self.l[0]!=self.com:
				self.ln+=1
				self.items=self.l.strip().split(self.sep)
				return True

	def getLine(self):
		return self.l
	
	def getItemsCnt(self):
		return len(self.items)

	def get(self, i):
		return self.items[i]
	
	def gets(self, types, indexes):
		try:
			return [tpe(self.items[idx]) for tpe,idx in zip(types, indexes)]
		except IndexError as e:
			print(e)
		return None

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

	def getShortDate(self, i):
		return datetime.strptime(self.items[i], '%Y-%m-%d')

	def getDate(self, i):
		return datetime.strptime(self.items[i], '%Y-%m-%d %H:%M:%S')
	
	def getDateFromTimestamp(self, i):
		return datetime.fromtimestamp(int(self.items[i]))
	
	def getTimestamp(self, i):
		d=self.getDate(i)
		return int(time.mktime(d.timetuple()))

	def getISODate(self, i):
		return datetime.strptime(self.items[i][:19], '%Y-%m-%dT%H:%M:%S')
	
	def getLN(self):
		return self.ln

class JsonStorer:
	def __init__(self, prefix, mx_rows=1000000, sufix='.json.gz', data_dir='data/'):
		self.fw=None
		self.writed=0
		self.mx_rows=mx_rows
		self.data_dir=data_dir
		if not os.path.exists(data_dir): os.mkdir(data_dir)
		self.prefix, self.sufix=prefix, sufix
	
	def __check(self):
		if self.writed>=self.mx_rows and self.fw is not None: self.close()
		if self.fw is None:
			self.fw=FileWriter('{}{}_{:.0f}{}'.format(self.data_dir, self.prefix, time.time(), self.sufix))
			self.writed=0

	def write(self, item):
		self.__check()
		self.fw.write(json.dumps(item)+'\n')
		self.writed+=1
		self.fw.flush()
	
	def close(self):
		if self.fw is not None: self.fw.close()
		self.fw=None
		self.writed=0

def saveList(slist, f, anno=None, com='#'):
	if type(f)==str:
		with FileWriter(f) as fw:
			fw.write(com+'file: '+f+'\n'+com+'Len: %d\n'%len(slist))
			if anno is not None: fw.write(anno.strip()+'\n')
			for e in slist: 
				if type(e) is tuple or type(e) is list: 
					fw.write('\t'.join(map(str, e))+'\n')
				else: fw.write(str(e)+'\n')
	else:
		for e in slist: 
			if type(e) is tuple or type(e) is list: 
				f.write('\t'.join(map(str, e))+'\n')
			else: f.write(str(e)+'\n')

def saveMap(tmap, fnm, anno=None, com='#'):
	with FileWriter(fnm) as fw:
		fw.write(com+'file: '+fnm+'\n')
		if anno is not None: fw.write(anno.strip()+'\n')
		for k,e in sorted(tmap.items()): 
			if type(e) is tuple or type(e) is list: fw.write(str(k)+'\t'+'\t'.join(map(str, e))+'\n')
			else: fw.write('%s\t%s\n' % (str(k), str(e)))

def loadList(fname, col=0, com='#'):
	rst=[]
	fio=FileIO(fname, com=com, echo=False)
	while fio.next(): rst.append(fio.getStr(col))
	return rst

def loadIntList(fname, col=0):
	rst=[]
	fio=FileIO(fname, echo=False)
	while fio.next(): rst.append(fio.getInt(col))
	return rst

def loadIntFltList(fname, c1=0, c2=1):
	rst=[]
	fio=FileIO(fname, echo=False)
	while fio.next(): rst.append((fio.getInt(c1),fio.getFlt(c2)))
	return rst

def loadFltList(fname, col=0):
	rst=[]
	fio=FileIO(fname, echo=False)
	while fio.next(): rst.append(fio.getFlt(col))
	return rst

def loadIntMap(fnm, ckey=0, cval=1):
	rst={}
	fio=FileIO(fnm, echo=False)
	while fio.next(): rst[fio.getInt(ckey)]=fio.getInt(cval)
	return rst

def loadStrIntMap(fnm, ckey=0, cval=1, com='#'):
	rst={}
	fio=FileIO(fnm, com=com, echo=False)
	while fio.next(): rst[fio.getStr(ckey)]=fio.getInt(cval)
	return rst

def loadIntStrMap(fnm, ckey=0, cval=1, com='#', sep='\t'):
	rst={}
	fio=FileIO(fnm, com=com, sep=sep, echo=False)
	while fio.next(): rst[fio.getInt(ckey)]=fio.getStr(cval)
	return rst

def loadIntFltMap(fnm, ckey=0, cval=1):
	rst={}
	fio=FileIO(fnm, echo=False)
	while fio.next(): rst[fio.getInt(ckey)]=fio.getFlt(cval)
	return rst

def loadStrFltMap(fnm, ckey=0, cval=1, com='#'):
	rst={}
	fio=FileIO(fnm, com=com, echo=False)
	while fio.next(): rst[fio.getStr(ckey)]=fio.getFlt(cval)
	return rst

def saveSet(slist, fnm, anno=None, com='#'):
	saveList(slist, fnm, anno=None, com='#')

def loadSet(fnm, c=0, com='#'):
	rst=set()
	fio=FileIO(fnm, com=com, echo=False)
	while fio.next(): rst.add(fio.getStr(c))
	return rst

def loadIntSet(fnm, c=0):
	rst=set()
	fio=FileIO(fnm, echo=False)
	while fio.next(): rst.add(fio.getInt(c))
	return rst

def loadFltSet(fnm, c=0):
	rst=set()
	fio=FileIO(fnm, echo=False)
	while fio.next(): rst.add(fio.getFlt(c))
	return rst

def loadIntPrSet(fnm, rst=None):
	if rst is None: rst=set()
	fio=FileIO(fnm,echo=False)
	while fio.next(): rst.add((fio.getInt(0), fio.getInt(1)))
	return rst

def loadIntPrList(fnm, rst=None):
	if rst is None: rst=[]
	fio=FileIO(fnm,echo=False)
	while fio.next(): rst.append((fio.getInt(0), fio.getInt(1)))
	return rst

def loadFltPrList(fnm, rst=None):
	if rst is None: rst=[]
	fio=FileIO(fnm,echo=False)
	while fio.next(): rst.append((fio.getFlt(0), fio.getFlt(1)))
	return rst

def loadIntFltPrMap(fnm):
	rst={}
	fio=FileIO(fnm, echo=False)
	while fio.next(): rst[fio.getInt(0)] = (fio.getFlt(1), fio.getFlt(2))
	return rst

def writeFile(data, fnm):
	if data is None or len(data)==0: return
	with FileWriter(fnm) as fw:
		fw.write(data)

def readFile(fnm):
	fr = FileReader(fnm)
	lines = fr.readlines()
	fr.close()
	return ''.join(lines)
