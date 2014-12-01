#! /usr/bin/env python3
#encoding: utf-8
import math

def nchoosek(n,k):
	return math.exp(math.lgamma(n+1)-math.lgamma(k+1)-math.lgamma(n-k+1))

#                         T(n+1)     T(k+alpah)T(n-k+beta)  T(alpha+beta)
# f(k|n,alpha,beta) = --------------.---------------------.---------------
#                     T(k+1)T(n-k+1)    T(n+alpha+beta)    T(alpha)T(beta)
def beta_binomial(k,n,alpha,beta):
	i1 = math.lgamma(n+1) - math.lgamma(k+1) - math.lgamma(n-k+1)
	i2 = math.lgamma(k+alpha) + math.lgamma(n-k+beta) - math.lgamma(n+alpha+beta)
	i3 = math.lgamma(alpha+beta) - math.lgamma(alpha) - math.lgamma(beta)
	return math.exp(i1+i2+i3)

if __name__=='__main__':
	N = 20
	print(sum([beta_binomial(i,N,0.27,9.73) for i in range(N+1)]))
