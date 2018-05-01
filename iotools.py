#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import bz2
import gzip
import json
import os
import sys
import time
import array
import struct
from datetime import datetime


def get_file_reader(fname, flag='rt'):
    ext = os.path.splitext(fname)[1]
    if ext == '.gz': return gzip.open(fname, flag)
    elif ext == '.bz2': return bz2.open(fname, flag)
    return open(fname, flag)


class FileWriter:
    def __init__(self, fname):
        ext = os.path.splitext(fname)[1]
        if ext == '.gz': self.f = gzip.open(fname, 'wt')
        elif ext == '.bz2': self.f = bz2.open(fname, 'wt')
        else: self.f = open(fname, 'w')

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def write(self, l):
        self.f.write(l)

    def flush(self):
        self.f.flush()

    def close(self):
        self.f.close()


class FileIO:
    def __init__(self, fname, sep='\t', com='#', echo=False):
        if echo: print('Loading file {} ...'.format(fname))
        sys.stdout.flush()
        self.sep, self.com, self.echo = sep, com, echo
        self.fr = get_file_reader(fname)
        self.ln = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.fr.close()
        if self.echo: print('totally', self.ln, 'lines.')
        return False

    def next(self):
        while True:
            self.l = self.fr.readline()
            if self.l and self.l[0] != self.com:
                self.ln += 1
                self.items = self.l.rstrip().split(self.sep)
                return True
            if not self.l:
                return False  # empty line = EOF

    def getLine(self):
        """ return original line with '\n' """
        return self.l

    def getItemsCnt(self):
        return len(self.items)

    def gets(self, types):
        return [fun(item) for fun, item in zip(types, self.items)]

    def getStrs(self):
        return self.items

    def getInts(self, idx=None):
        return [int(item) for item in self.items] if idx is None else \
            [int(self.items[i]) for i in idx]

    def getFlts(self, idx=None):
        return [float(item) for item in self.items] if idx is None else \
            [float(self.items[i]) for i in idx]

    def get(self, i, fun=str):
        return fun(self.items[i])

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
        d = self.getDate(i)
        return int(time.mktime(d.timetuple()))

    def getISODate(self, i):
        return datetime.strptime(self.items[i][:19], '%Y-%m-%dT%H:%M:%S')

    def getLN(self):
        return self.ln


class JsonStorer:
    def __init__(self, prefix, mx_rows=1E6, sufix='.json.gz',
                 data_dir='data/'):
        self.fw = None
        self.writed = 0
        self.mx_rows = mx_rows
        self.data_dir = data_dir
        if not os.path.exists(data_dir): os.mkdir(data_dir)
        self.prefix, self.sufix = prefix, sufix

    def __check(self):
        if self.writed >= self.mx_rows and self.fw is not None: self.close()
        if self.fw is None:
            self.fw = FileWriter('{}{}_{:.0f}{}'.format(
                self.data_dir, self.prefix, time.time(), self.sufix))
            self.writed = 0

    def write(self, item):
        if item is None: return
        self.__check()
        self.fw.write(json.dumps(item) + '\n')
        self.writed += 1
        self.fw.flush()

    def close(self):
        if self.fw is not None: self.fw.close()
        self.fw = None
        self.writed = 0


def get_format(e):
    if isinstance(e, tuple) or isinstance(e, list):
        tmp = [
            '{0[%d]:.6e}' % i if isinstance(val, float) else '{0[%d]}' % i
            for i, val in enumerate(e)
        ]
        fmt = '\t'.join(tmp)
    else:
        fmt = '{:.6e}' if isinstance(e, float) else '{}'
    return fmt + "\n"


# savers
def saveList(data, filename):
    with FileWriter(filename) as fw:
        it = iter(data)
        e = next(it)
        fmt = get_format(e)
        fw.write(fmt.format(e))
        for e in it:
            fw.write(fmt.format(e))
    print("saved to", filename)


def saveList(data, filename, fmt):
    ''' example fmt = "{0[0]}\t{0[1]:.2f}\t{0[2]}\n" '''
    with FileWriter(filename) as fw:
        for e in data:
            fw.write(fmt.format(e))
    print("saved to", filename)


def saveMap(tmap, filename):
    saveList(tmap.items(), filename)


def saveSet(slist, filename):
    saveList(slist, filename)


# loader
def loadList(fname, col=0, fun=str):
    rst = []
    with FileIO(fname) as fio:
        while fio.next():
            rst.append(fio.get(col, fun))
    return rst


def loadIntList(fname, col=0):
    return loadList(fname, col, int)


def loadFltList(fname, col=0):
    return loadList(fname, col, float)


def loadTupleList(fname, funs):
    rst = []
    with FileIO(fname) as fio:
        while fio.next():
            rst.append(tuple([fio.get(i, f) for i, f in enumerate(funs)]))
    return rst


def loadIntIntList(fname):
    return loadTupleList(fname, (int, int))


def loadIntFltList(fname):
    return loadTupleList(fname, (int, float))


def loadMap(filename, ckey=0, cval=1, k_type=str, v_type=str):
    rst = {}
    with FileIO(filename) as fio:
        while fio.next():
            rst[fio.get(ckey, k_type)] = fio.get(cval, v_type)
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


def loadSet(filename, col=0, fun=str):
    rst = set()
    with FileIO(filename) as fio:
        while fio.next():
            rst.add(fio.get(col, fun))
    return rst


def loadIntSet(filename, col=0):
    return loadSet(filename, col, int)


def loadFltSet(filename, col=0):
    return loadSet(filename, col, float)


def loadMultiColSet(filename, cols, fun=str):
    rst = set()
    with FileIO(filename) as fio:
        while fio.next():
            for c in cols:
                rst.add(fio.get(c, fun))
    return rst


def loadIntPrSet(filename, rst=None):
    if rst is None:
        rst = set()
    with FileIO(filename) as fio:
        while fio.next():
            rst.add((fio.getInt(0), fio.getInt(1)))
    return rst


def loadIntPrList(filename, rst=None):
    if rst is None:
        rst = []
    with FileIO(filename) as fio:
        while fio.next():
            rst.append((fio.getInt(0), fio.getInt(1)))
    return rst


def loadStrPrList(filename, rst=None):
    if rst is None:
        rst = []
    with FileIO(filename) as fio:
        while fio.next():
            rst.append((fio.getStr(0), fio.getStr(1)))
    return rst


def loadFltPrList(filename, rst=None):
    if rst is None:
        rst = []
    with FileIO(filename) as fio:
        while fio.next():
            rst.append((fio.getFlt(0), fio.getFlt(1)))
    return rst


def loadIntsList(filename, rst=None):
    if rst is None:
        rst = []
    with FileIO(filename) as fio:
        while fio.next():
            rst.append(fio.getInts())
    return rst


def loadIntFltPrMap(filename):
    rst = {}
    with FileIO(filename) as fio:
        while fio.next():
            rst[fio.getInt(0)] = (fio.getFlt(1), fio.getFlt(2))
    return rst


def writeFile(data, filename):
    if data is None or len(data) == 0: return
    with FileWriter(filename) as fw:
        fw.write(data)


def readFile(filename):
    fr = get_file_reader(filename)
    lines = fr.readlines()
    fr.close()
    return ''.join(lines)


def load_array(fnm, dtype='i'):
    """ format: [len, item1, item2, ...] """
    a = array.array(dtype)
    with get_file_reader(fnm, 'rb') as f:
        a.fromfile(f, struct.unpack("i", f.read(4))[0])
    return a
