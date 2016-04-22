#! /usr/bin/env python
#encoding: utf-8

import math
from collections import Counter
from iotools import FileIO, FileWriter

def summary(data):
    import numpy
    from scipy.stats import scoreatpercentile
    q1 = scoreatpercentile(data,25)
    q3 = scoreatpercentile(data,75)
    md = numpy.median(data)
    return (min(data), q1, md, q3, max(data))

def stat_all(samples, fnm='stats.dat', sep='\t'):
    hasWeight = type(samples[0]) is tuple or type(samples[0]) is list
    if hasWeight: minX, maxX = min(samples)[0], max(samples)[0]
    else: minX, maxX = min(samples), max(samples)
    L=maxX+1
    pdf, cdf, ccdf=[0]*L, [0]*L, [0]*(L+1)
    sumv=0
    if hasWeight:
        for v, w in samples:
            pdf[v]+=w
            ccdf[v]+=w
            cdf[v]+=w
            sumv+=w
    else:
        for v in samples:
            pdf[v]+=1
            ccdf[v]+=1
            cdf[v]+=1
            sumv+=1
    for i in range(L-2,0,-1): ccdf[i]+=ccdf[i+1]
    for i in range(1, L): cdf[i]+=cdf[i-1]
    exp=var=0
    for i in range(L):
        pdf[i]/=sumv
        cdf[i]/=sumv
        ccdf[i]/=sumv
        exp+=pdf[i]*i
        var+=pdf[i]*i*i
    fw=FileWriter(fnm)
    fw.write('#min X: {:d}\n'.format(minX))
    fw.write('#max X: {:d}\n'.format(maxX))
    fw.write('#total: {:.2f}\n'.format(sumv))
    fw.write('#expectation: {:.2f}\n'.format(exp))
    fw.write('#variance: {:.6f}\n'.format(math.sqrt(var-exp*exp)))
    fw.write('#X, FREQ, PDF, CDF, CCDF\n')
    for i in range(L):
        if pdf[i]+ccdf[i+1]>1E-10:
            fw.write('{1:d}{0}{2:d}{0}{3:.6e}{0}{4:.6e}{0}{5:.6e}\n'.format(sep, i, int(pdf[i]*sumv), pdf[i], cdf[i], ccdf[i+1]))
    fw.close()

def get_pdf(samples, fnm=None):
    num_samples = len(samples)
    cnt = Counter()
    for s in samples: cnt[s] += 1
    pdf = [(key,val/num_samples) for key,val in cnt.items()]
    pdf.sort(key=lambda e: e[0])
    if fnm is not None:
        with open(fnm, 'w') as fw:
            for k,v in pdf: fw.write('{:d}\t{:.6e}\n'.format(k,v))
    return pdf

def get_pdf_flt(samples, fnm=None):
    ''' elements in samples are positive floats '''
    N = len(samples)
    cnt = Counter()
    for s in samples:
        key = int(math.floor(s))
        cnt[key] += 1
    pdf = [(k,v/N) for k,v in cnt.items()]
    pdf.sort(key=lambda e: e[0])
    if fnm is not None:
        with open(fnm, 'w') as fw:
            for k,v in pdf: fw.write('{:d}\t{:.6e}\n'.format(k,v))
    return pdf

def get_ccdf(pdf, fnm=None, sep='\t'):
    ''' Input: pdf is a list of tuple [(x, px), ...].
            x must be an integer, and x>=0.
            The pdf does not need to be normlized.
        Return: a CCDF list, [(-1,P(X>-1)), (0,P(X>0)), ...]
    '''
    ccdf = [y for x,y in pdf]
    for i in range(len(ccdf)-2,-1,-1): ccdf[i] += ccdf[i+1]
    print(ccdf)
    for i in range(len(ccdf)-1,-1,-1): ccdf[i] /= ccdf[0]
    if fnm is not None:
        fw=FileWriter(fnm)
        for e,y in zip(pdf,ccdf):
            fw.write('{1:d}{0}{2:.6e}\n'.format(sep, e[0]-1, y))
        fw.close()
    return ccdf

def get_ccdf_from_samples(sampels, fnm):
    pdf = get_pdf(samples)
    get_ccdf(pdf, fnm)

def get_cdf(pdf, fnm=None, sep="\t"):
    ''' pdf is a list [(x, px)...], and return list [P(X<=x)]. '''
    cdf = [p for x,p in pdf]
    for i in range(1, len(cdf)): cdf[i] += cdf[i-1]
    if fnm is not None:
        fw=FileWriter(fnm)
        for e,y in zip(pdf,cdf):
            fw.write('{1:d}{0}{2:.6e}\n'.format(sep, e[0], y))
        fw.close()
    return cdf

def get_frequency(keys):
    '''state the frequency of keys'''
    freq = Counter()
    for key in keys: freq[key] += 1
    return tuple(freq.values())

if __name__=='__main__':
    #statAll([1,1,2,3,4,6])
    pdf = [(0,.3), (1,.2), (5,.5)]
    print(get_cdf(pdf, 'test'))
