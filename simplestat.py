#! /usr/bin/env python
#encoding: utf-8
from iotools import FileIO, FileWriter
import math

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
    N = len(samples)
    dist = {}
    for s in samples:
        try:
            dist[s]+=1
        except KeyError:
            dist[s]=1
    pdf = [(k,v/N) for k,v in dist.items()]
    pdf.sort(key=lambda e: e[0])
    if fnm is not None:
        with open(fnm, 'w') as fw:
            for k,v in pdf: fw.write('{:d}\t{:.6e}\n'.format(k,v))
    return pdf


def get_pdf_flt(samples, fnm=None):
    ''' elements in samples are positive floats '''
    N = len(samples)
    dist = {}
    for s in samples:
        key = int(math.floor(s))
        try:
            dist[key] += 1
        except KeyError:
            dist[key]=1
    pdf = [(k,v/N) for k,v in dist.items()]
    pdf.sort(key=lambda e: e[0])
    if fnm is not None:
        with open(fnm, 'w') as fw:
            for k,v in pdf: fw.write('{:d}\t{:.6e}\n'.format(k,v))
    return pdf

def get_ccdf(pdf, fnm=None, sep='\t'):
    ''' Input: pdf is a list of tuple [(x, px), ...].
            x must be an integer, and x>=0.
            The pdf does not need to be normlized.
        Return: a CCDF list, [P(X>-1), P(X>0), ..., P(X>max_X-1)]
    '''
    L = max(pdf, key=lambda x: x[0])[0] + 1
    ccdf =[0]*L
    for x,y in pdf: ccdf[x]=y
    for x in range(L-2, -1, -1): ccdf[x]+=ccdf[x+1]
    for x in range(L-1, -1, -1): ccdf[x]/=ccdf[0]
    if fnm is not None:
        fw=FileWriter(fnm)
        for i in range(L):
            fw.write('{1:d}{0}{2:.6e}\n'.format(sep, i-1, ccdf[i]))
        fw.close()
    return ccdf

def get_ccdf_from_samples(sampels, fnm):
    pdf = get_pdf(samples)
    get_ccdf(pdf, fnm)

# pdf is a list of tuple [(x, px)...]
def get_cdf(pdf):
    L=max(pdf, key=lambda x: x[0])[0]+1
    cdf=[0]*L
    for x,p in pdf: cdf[x]=p
    for i in range(1, L): cdf[i]+=cdf[i-1]
    for i in range(L): cdf[i]/=cdf[-1]
    return cdf

if __name__=='__main__':
    #statAll([1,1,2,3,4,6])
    pdf = [(0,3), (1,2), (5,1)]
    print(get_ccdf(pdf, 'test'))
