#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import re
import os

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
    else: return int(time.time() * 1000)


def pretty_number(x):
    if x < 1000: return str(x)
    elif x < 1000000: return '{:d}K'.format(x // 1000)
    elif x < 1000000000: return '{:d}M'.format(x // 1000000)
    else: return '{:d}B'.format(x // 1000000000)


def pretty_time(x):
    if x < 60: return '{:.2f} secs'.format(x)
    elif x < 3600: return '{:.2f} mins'.format(x / 60)
    else: return '{:.2f} hrs'.format(x / 3600)


def pretty_size(x):
    if x < 1024: return str(x)
    elif x < 2**20: return '{:d}K'.format(x // 2**10)
    elif x < 2**30: return '{:d}M'.format(x // 2**20)
    else: return '{:d}G'.format(x // 2**30)


def insert_suffix(fnm, suffix):
    """ /dir/to/file.ext -> /dir/to/file_sfx.ext """
    name, ext = os.path.splitext(fnm)
    return "{}_{}{}".format(name, suffix, ext)


class Timer:
    def __init__(self):
        self.tick()

    def tick(self):
        self.start = time.time()

    def tmstr(self):
        tme = time.time() - self.start
        return pretty_time(tme)


if __name__ == '__main__':
    print(insert_suffix("/this/is/a/test.txt", "hello"))
    print(pretty_size(2**29))
