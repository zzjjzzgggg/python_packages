#! /usr/bin/env python
#encoding: utf-8

import time
import re

REX_NUM = re.compile(r'\d+')
REX_MONEY = re.compile(r'[$￥\d\.]+')
def get_nums(ss, ignore_comma=False):
    if ignore_comma: ss = ss.replace(',', '')
    return list(map(int, REX_NUM.findall(ss)))

def get_first_num(ss, ignore_comma=False):
    nums = get_nums(ss, ignore_comma)
    return 0 if len(nums) == 0 else nums[0]

def get_num(ss):
    return get_first_num(ss, True)

def get_last_num(ss, ignore_comma=False):
    nums = get_nums(ss, ignore_comma)
    return 0 if len(nums) == 0 else nums[-1]

def get_money(ss):
    ms = REX_MONEY.findall(ss)
    return None if len(ms) == 0 else ms[0]

def timestamp(sec=True):
    if sec: return int(time.time())
    else: return int(time.time()*1000)

def pretty_number(x):
    ''' Format x '''
    if x < 1000: return str(x)
    elif x < 1000000: return '{:d}K'.format(x//1000)
    elif x < 1000000000: return '{:d}M'.format(x//1000000)
    else: return '{:d}B'.format(x//1000000000)

def pretty_time(x):
    if x < 60: return '{:.2f} secs'.format(x)
    elif x < 3600: return '{:.2f} mins'.format(x/60)
    else: return '{:.2f} hrs'.format(x/3600)

class Timer:
    def __init__(self):
        self.tick()

    def tick(self):
        self.start = time.time()

    def tmstr(self):
        tme = time.time() - self.start
        return pretty_time(tme)

if __name__ == '__main__':
    #print(get_nums('asdfsa_12,34_asdf_456aaa34.55'))
    #print(getMoney('asdf￥9.90asdfa'))
    #print(timestamp())
    print(pretty_number(60876))
