import hashlib
import logging
import re
logger = logging.getLogger('main.util')


def gen_id(string):
    return int(hashlib.md5(str.encode(string)).hexdigest(), 16)


def groupby_na_to_zero(df, ticker):
    df = df.groupby(ticker).first()
    df.fillna(0, inplace=True)
    return df


def missing_ticker(index):
    tickers = set()
    rx = re.compile("\((.+)\)")
    fh = open('log.log', 'r')
    for line in fh:
        if 'Found duplicate' not in line:
            strings = re.findall(rx, line)
            if strings and index in strings[0]:
                    tickers.add(strings[0].split(',')[1])
    logger.info('Found %d missing quotes in %s' % (len(tickers), index))
    fh.close()
    return list(tickers)


def find_dateByVolume(df, tdate, fvolume, direction):
    i = df.index.get_loc(tdate)
    sum = df.iloc()[i]['volume']
    while sum < fvolume:
        if direction == 'backward':
            sum = df.iloc()[i-1]['volume'] + sum
            i = i-1
        elif direction == 'forward':
            sum = df.iloc()[i+1]['volume'] + sum
            i = i+1
    return df.iloc()[i]


def distance_date(items, pivot, distance):
    if distance == "nearest":
        return min(items, key=lambda x: abs(x - pivot))
    elif distance == "farest":
        return max(items, key=lambda x: abs(x - pivot))
