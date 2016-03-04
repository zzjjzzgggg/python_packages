#! /usr/bin/env python
#encoding: utf-8

from datetime import datetime
import os, sys, time, json, bz2, gzip

class FileReader:
    def __init__(self, fname):
        ext = os.path.splitext(fname)[1]
        if ext =='.gz': self.fr=gzip.open(fname, 'rt')
        elif ext == '.bz2': self.fr=bz2.open(fname, 'rt')
        else: self.fr=open(fname)

    def readline(self):
        l = self.fr.readline()
        return l.strip() if l else None

    def readlines(self):
        return self.fr.readlines()

    def close(self):
        self.fr.close()

class FileWriter:
    def __init__(self, fname):
        ext = os.path.splitext(fname)[1]
        if ext =='.gz': self.f=gzip.open(fname, 'wt')
        elif ext == '.bz2': self.f=bz2.open(fname, 'wt')
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
        if echo: print('Loading file {} ...'.format(fname))
        sys.stdout.flush()
        self.sep=sep
        self.com=com
        self.ln=0
        self.fr=FileReader(fname)
        self.echo=echo

    def __enter__(self):
        return self

    def __exit__(self,*exc):
        self.fr.close()
        if self.echo: print('totally', self.ln, 'lines.')
        return False

    def next(self):
        while True:
            self.l=self.fr.readline()
            if self.l is None: return False # EOF
            if len(self.l)>0 and self.l[0]!=self.com:
                self.ln+=1
                self.items=self.l.split(self.sep)
                return True

    def getLine(self):
        return self.l

    def getItemsCnt(self):
        return len(self.items)

    def gets(self, types):
        return [fun(item) for fun,item in zip(types,self.items)]

    def getStrs(self):
        return self.items

    def getInts(self):
        return self.gets(int)

    def getFlts(self):
        return self.gets(float)

    def get(self, i, type_fun):
        return type_fun(self.items[i])

    def getStr(self, i):
        return self.items[i]

    def getInt(self, i=0):
        return self.get(i, int)

    def getFlt(self, i):
        return self.get(i, float)

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
    def __init__(self, prefix, mx_rows=1E6, sufix='.json.gz', data_dir='data/'):
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
        if item is None: return
        self.__check()
        self.fw.write(json.dumps(item)+'\n')
        self.writed+=1
        self.fw.flush()

    def close(self):
        if self.fw is not None: self.fw.close()
        self.fw=None
        self.writed=0

# savers
def saveList(slist, filename):
    with FileWriter(filename) as fw:
        for e in slist:
            if type(e) is not tuple and type(e) is not list:
                fw.write(str(e) + '\n')
            else:
                fw.write('\t'.join(map(str, e)) + '\n')

def saveXFltList(dat, filename):
    with FileWriter(filename) as fw:
        for x,y in dat:
            fw.write('{}\t{:.6e}\n'.format(x, y))

def saveMap(tmap, filename):
    with FileWriter(filename) as fw:
        for k,e in tmap.items():
            if type(e) is tuple or type(e) is list:
                fw.write(str(k) + '\t' + '\t'.join(map(str, e)) + '\n')
            else:
                fw.write('{}\t{}\n'.format(k, e))

def saveSet(slist, filename):
    saveList(slist, filename)

# loader
def loadList(fname, col=0, type_fun=str):
    rst=[]
    with FileIO(fname, echo=False) as fio:
        while fio.next():
            rst.append(fio.get(col, type_fun))
    return rst

def loadIntList(fname, col=0):
    return loadList(col, int)

def loadFltList(fname, col=0):
    return loadList(col, float)

def loadIntFltList(fname, c1=0, c2=1):
    rst=[]
    with FileIO(fname, echo=False) as fio:
        while fio.next():
            rst.append((fio.getInt(c1),fio.getFlt(c2)))
    return rst

def loadMap(filename, ckey=0, cval=1, key_type=str, val_type=str):
    rst={}
    with FileIO(filename, echo=False) as fio:
        while fio.next():
            rst[fio.get(ckey, key_type)] = fio.get(cval, val_type)
    return rst

def loadIntMap(filename, ckey=0, cval=1):
    return loadMap(filename, ckey, cval, int, int)

def loadStrIntMap(filename, ckey=0, cval=1):
    return loadMap(filename, ckey, cval, str, int)

def loadIntStrMap(filename, ckey=0, cval=1):
    return loadMap(filename, ckey, cval, int, str)

def loadIntFltMap(filename, ckey=0, cval=1):
    return loadMap(filename, ckey, cval, int, float)

def loadStrFltMap(filename, ckey=0, cval=1):
    return loadMap(filename, ckey, cval, str, float)

def loadSet(filename, c=0, type_fun=str):
    rst=set()
    with FileIO(filename, com=com, echo=False) as fio:
        while fio.next(): rst.add(fio.get(c, str))
    return rst

def loadIntSet(filename, c=0):
    return loadSet(filename, c, int)

def loadFltSet(filename, c=0):
    return loadSet(filename, c, float)

def loadMultiColSet(filename, cols, type_fun=str):
    rst=set()
    with FileIO(filename, echo=False) as fio:
        while fio.next():
            for c in cols:
                rst.add(fio.get(c, type_fun))
    return rst


def loadIntPrSet(filename, rst=None):
    if rst is None: rst=set()
    with FileIO(filename,echo=False) as fio:
        while fio.next(): rst.add((fio.getInt(0), fio.getInt(1)))
    return rst

def loadIntPrList(filename, rst=None):
    if rst is None: rst=[]
    with FileIO(filename,echo=False) as fio:
        while fio.next(): rst.append((fio.getInt(0), fio.getInt(1)))
    return rst

def loadStrPrList(filename, rst=None):
    if rst is None: rst=[]
    with FileIO(filename,echo=False) as fio:
        while fio.next(): rst.append((fio.getStr(0), fio.getStr(1)))
    return rst

def loadFltPrList(filename, rst=None):
    if rst is None: rst=[]
    with FileIO(filename,echo=False) as fio:
        while fio.next(): rst.append((fio.getFlt(0), fio.getFlt(1)))
    return rst


def loadIntsList(filename, rst=None):
    if rst is None: rst=[]
    with FileIO(filename,echo=False) as fio:
        while fio.next(): rst.append(fio.getInts())
    return rst

def loadIntFltPrMap(filename):
    rst={}
    with FileIO(filename, echo=False) as fio:
        while fio.next(): rst[fio.getInt(0)] = (fio.getFlt(1), fio.getFlt(2))
    return rst





def writeFile(data, filename):
    if data is None or len(data)==0: return
    with FileWriter(filename) as fw: fw.write(data)

def readFile(filename):
    fr = FileReader(filename)
    lines = fr.readlines()
    fr.close()
    return ''.join(lines)
