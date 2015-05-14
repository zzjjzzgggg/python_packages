#! /usr/bin/env python3
#encoding: utf-8

# correct errors in Json string

import json
import io
import tokenize
import token

def fixLazyJson(in_text):
    tokengen = tokenize.generate_tokens(io.StringIO(in_text).readline)
    result = []
    for tokid, tokval, _, _, _ in tokengen:
        # fix unquoted strings
        if (tokid == token.NAME):
            if tokval not in ['true', 'false', 'null', '-Infinity', 'Infinity', 'NaN']:
                tokid = token.STRING
                tokval = u'"%s"' % tokval
        # fix single-quoted strings
        elif (tokid == token.STRING):
            if tokval.startswith ("'"):
                tokval = u'"%s"' % tokval[1:-1].replace ('"', '\\"')
        # remove invalid commas
        elif (tokid == token.OP) and ((tokval == '}') or (tokval == ']')):
            if (len(result) > 0) and (result[-1][1] == ','):
                result.pop()
        # fix single-quoted strings
        elif (tokid == token.STRING):
            if tokval.startswith ("'"):
                tokval = u'"%s"' % tokval[1:-1].replace ('"', '\\"')
        result.append((tokid, tokval))
    return tokenize.untokenize(result)

def loads(text):
    obj = None
    try:
        obj = json.loads(text)
    except ValueError:
        fmted_str = fixLazyJson(text)
        obj = json.loads(fmted_str)
    return obj

if __name__=='__main__':
    #print(fixLazyJson('{a:1}'))
    import json
    with open('following.txt') as fr: lines = fr.readlines()
    cont = ''.join(lines)
    p1, p2 = cont.find('{'), cont.rfind('}')+1
    try:
        obj = json.loads(cont[p1:p2])
    except ValueError:
        fmted_str = fixLazyJson(cont[p1:p2])
        obj = json.loads(fmted_str)
