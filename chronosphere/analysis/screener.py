import json
import logging
import re
import time

import pandas as pd
import requests
import yfinance as yf
from yahooquery import Ticker as yhqT


from ..models import Index, Watchlist_Index
from .email import sendMail
from ..utils.config import Config
from ..utils.utils import get_smarter_session

logger = logging.getLogger('main.screener')
pd.set_option('mode.chained_assignment', None)


def screener_analysis(sdic):
    picks_dic = {}
    for dbname, s in sdic.items():
        if dbname in ('tsxci', 'nasdaq100', 'sp100', 'csi300', 'eei', 'commodity'):
            logger.info("Start to process: %s" % dbname)
            # Get tickers
            tickers = [r.symbol for r in s.query(Index.symbol).distinct()]
            picks_list = []
        elif dbname in ('financials'):
            logger.info("Start to process: %s" % dbname)
            # Get tickers
            tickers = [r.symbol for r in s.query(Watchlist_Index.symbol).distinct()]
            picks_list = []

            # Iterate tickers
            for ticker in tickers:
            # for ticker in ['POW.TO']:
                try:
                    # Key Stats and Current Price
                    if dbname == "tsxci":
                        ks = get_keyStat(dbname, ticker + ".TO")
                    elif dbname == 'csi300':
                        ticker = ticker.replace('SH', 'SS')
                        ks = get_keyStat(dbname, ticker)
                    else:
                        ks = get_keyStat(dbname, ticker)
                    # print(ks)
                    # print(dbname)
                    # print(ks['pe'])
                    # print(ks['pb'])


                    # Screener
                    pe = ks['pe']
                    pb = ks['pb']
                    eps = ks['eps']
                    de_ratio = ks['de']
                    ps = ks['ps']
                    dividendYield = ks['dy']
                    payoutRatio = ks['por']

                    # Screener conditions
                    # China
                    # if dbname == 'csi300' and eps > 0 and pe <= 15 and pb <= 1.59 and ps <= 2 and\
                    #   de_ratio <= 50 and dividendYield >= 4.05 and payoutRatio >= 25:
                    #     picks_list.append(ticker)
                    # # Everywhere else
                    # elif eps > 0 and pe <= 15 and pb <= 1.59 and ps <= 2 and\
                    #   de_ratio <= 50 and dividendYield >= 4.05 and payoutRatio >= 25 and ks['beta'] <= 1.2:
                    #     picks_list.append(ticker)
                    #     print(11111111111111111111)
                    if dbname == 'financials' and pe <= 15 and pb <= 1.50:
                        picks_list.append(ticker)

                    logger.info("Screening - (%s, %s)" % (dbname, ticker))
                except:
                    logger.info("Failed to collect - (%s, %s)" % (dbname, ticker))
                    pass

            if len(picks_list) > 0:
                picks_dic.update({dbname: picks_list})
                logger.info("Screener found - (%s, %s)" % (dbname, picks_list))
    print(picks_dic)
    sendMail(Config, picks_dic)


def get_up_down_ratio(df):
    df = df[(df != 0).all(1)]
    # Latest Close
    df = df.sort_index(ascending=True).drop(columns=['id'])
    df_latest = df.iloc[-1]
    latest_date = df_latest.name
    latest_close = df_latest['close']

    # 6 Month ratio
    total_6m = len(df.last('6m'))
    up_6m = len(df.last('6m')[df.last('6m')['close'] > latest_close])
    down_6m = len(df.last('6m')[df.last('6m')['close'] < latest_close])
    up_6m_ratio = int(round(up_6m/total_6m,2)*100)
    down_6m_ratio = int(round(down_6m/total_6m,2)*100)

    # 1 Year ratio
    total_1y = len(df.last('12m'))
    up_1y = len(df.last('12m')[df.last('12m')['close'] > latest_close])
    down_1y = len(df.last('12m')[df.last('12m')['close'] < latest_close])
    up_1y_ratio = int(round(up_1y/total_1y,2)*100)
    down_1y_ratio = int(round(down_1y/total_1y,2)*100)

    # 5 Year Ratio
    total_5y = len(df.last('60m'))
    up_5y = len(df.last('60m')[df.last('60m')['close'] > latest_close])
    down_5y = len(df.last('60m')[df.last('60m')['close'] < latest_close])
    up_5y_ratio = int(round(up_5y/total_5y,2)*100)
    down_5y_ratio = int(round(down_5y/total_5y,2)*100)
    try:
        sixm = up_6m_ratio / down_6m_ratio
    except ZeroDivisionError:
        sixm = -1
    try:
        oney = up_1y_ratio/down_1y_ratio
    except ZeroDivisionError:
        oney = -1
    try:
        fivey = up_5y_ratio/down_5y_ratio
    except ZeroDivisionError:
        fivey = -1
    return {'6m': round(sixm, 2),
            '1y': round(oney, 2),
            '5y': round(fivey, 2)
           }


def get_keyStat(dbname, ticker):

    data = {}

    tinfo = None

    if tinfo is not None:
        pass
    else:
        # Supplement Stock Chart summary and yahooquery
        try:
            scs, sck = stockChartSummary(ticker)
        except:
            scs = None
            sck = None
        if scs is not None and tinfo is None:
            if scs['EPS'] not in ('-','',None):
                eps = float(scs['EPS'])
            else:
                eps = None
            if scs['Dividend Yield'] not in ('-','',None):
                dy = float(scs['Dividend Yield'])
            else:
                dy = None
            if scs['Dividend Rate'] not in ('-','',None):
                dr = float(scs['Dividend Rate'])
            else:
                dr = None

            try:
                if scs['Price To Book'] not in ('-','',None):
                    pb = float(scs['Price To Book'])
                else:
                    pb = None
            except:
                pb = None

            if scs['Beta60Month'] not in ('-','',None):
                beta = float(scs['Beta60Month'])
            else:
                beta = None
            if scs['PriceToFreeCashFlow'] not in ('-','',None):
                priceToFreeCashFlow = float(scs['PriceToFreeCashFlow'])
            else:
                priceToFreeCashFlow = None
            if scs['PERatio'] not in ('-','',None):
                pe = float(scs['PERatio'])
            else:
                pe = None

            if sck['close'] not in ('-','',None):
                close = float(sck['close'])
            else:
                close = None

            data.update({
                'bvps': 0,
                'cr': 1,
                'qr':'NA',
                'de': 50,
                'ps': 1,
                'pb': pb,
                'eps': eps,
                'dy': dy,
                'por': 'NA',
                'dr': dr,
                'pe': pe,
                'deltaDR_PE': 'NA',
                'ebitdaMargins': 'NA',
                'profitMargins': 'NA',
                'grossMargins': 'NA',
                'operatingMargins': 'NA',
                'earningsQuarterlyGrowth': 'NA',
                'revenueQuarterlyGrowth': 'NA',
                'beta': beta,
                'roa': 'NA',
                'roe': 'NA',
                'priceToFreeCashFlow': priceToFreeCashFlow,
                'close':close,
            })
    try:
        try:
            mc = millify(sck['marketCap'])
        except:
            mc = 'NA'
        # Moving Average
        twoHundredDayAverage, fiftyDayAverage = (0, 0)
        data.update({
            'mc':mc,
            'twoHundredDayAverage': twoHundredDayAverage,
            'fiftyDayAverage': fiftyDayAverage,
        })
    except:
        data.update({
            'mc': 'NA',
            'twoHundredDayAverage': 0,
            'fiftyDayAverage': 0,
        })


    # BVPS
    try:
            data.update({'bvps': round(data['close'] / data['pb'], 2)})
    except:
        pass

    return data



def _get_headers():
    return {"accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en;q=0.9",
            "cache-control": "no-cache",
            "dnt": "1",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}

def stockChartSummary(ticker):
    if '-' in ticker:
        ticker = ticker.replace('-', '%2F')
    url = 'https://stockcharts.com/j-sum/sum?cmd=symsum&symbol={0}'.format(ticker)
    try:
        html = requests.get(url, headers=_get_headers()).text
    except:
        time.sleep(30)
        html = requests.get(url, headers=_get_headers()).text
    # Cleaning
    try:
        data = json.loads(html)
        funda = data['fundamentals']
        del funda['date']
        # Oct 30, 2022
        # funda.update(float({'SCTR':round(data['SCTR'],2)}))
        # funda.update({'Annual Dividend Yield':data['yield']})
        # funda.update({'Sector Name':data['sectorName']})
        for key, item in funda.items():
            funda[re.sub(r"(?<=[a-z])(?=[A-Z])", " ", key)] = funda.pop(key)
        return funda, data
    except:
        pass


def get_query1_yfinance(ticker):
    url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ticker
    try:
        data = json.loads(requests.get(url, headers=_get_headers()).text)
    except:
        time.sleep(30)
        data = requests.get(url, headers=_get_headers()).text
    try:
        fin_data = data['quoteResponse']['result'][0]
        return fin_data
    except:
        pass