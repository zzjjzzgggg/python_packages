#! /usr/bin/env python3
#encoding: utf-8
import math

def nchoosek(n,k):
	return math.exp(math.lgamma(n+1)-math.lgamma(k+1)-math.lgamma(n-k+1))

def beta_binomial(x,n,alpha,beta):
	i1 = math.lgamma(-alpha+1) - math.lgamma(x+1) - math.lgamma(-alpha-x+1)
	i2 = math.lgamma(-beta+1) - math.lgamma(n-x+1) - math.lgamma(-beta-n+x+1)
	i3 = math.lgamma(-alpha-beta+1) - math.lgamma(n+1) - math.lgamma(-alpha-beta-n+1)
	return math.exp(i1+i2-i3)

if __name__=='__main__':
	print(sum([beta_binomial(i,10,0.4,0.5) for i in range(11)]))
