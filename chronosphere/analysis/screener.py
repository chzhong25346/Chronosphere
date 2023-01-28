import json
import logging
import re
import time

import pandas as pd
import requests
import yfinance as yf

from ..models import Index
from .email import sendMail
from ..utils.config import Config

logger = logging.getLogger('main.screener')
pd.set_option('mode.chained_assignment', None)

def screener_analysis(sdic):
    picks_dic = {}
    for dbname, s in sdic.items():
        if dbname in ('tsxci','nasdaq100','sp100','csi300','eei','commodity'):
            logger.info("Start to process: %s" % dbname)
            # Get tickers
            tickers = [r.symbol for r in s.query(Index.symbol).distinct()]
            picks_list = []

            # Iterate tickers
            for ticker in tickers:
            # for ticker in ['BNS']:
                try:
                    # Key Stats and Current Price
                    if dbname == "tsxci":
                        keyStat = get_keyStat(dbname, ticker+".TO")
                    elif dbname == 'csi300':
                        ticker = ticker.replace('SH', 'SS')
                        keyStat = get_keyStat(dbname, ticker)
                    else:
                        keyStat = get_keyStat(dbname, ticker)

                    # Screener
                    ps_ratio = keyStat['ps']
                    pe = keyStat['pe']
                    pb = keyStat['pb']
                    ps = keyStat['ps']
                    eps = keyStat['eps']
                    de_ratio = keyStat['de']
                    dividendYield = keyStat['dy']*100
                    payoutRatio = keyStat['por']
                    beta5yMonthly = keyStat['beta5yMonthly']
                    marketCap = keyStat['mc']

                    # print(dbname, eps,pe,pb,de_ratio,dividendYield,payoutRatio,beta5yMonthly,marketCap)

                    # Screener conditions
                    # China
                    if dbname == 'csi300' and eps > 0 and pe <= 15 and pb <= 1.59 and\
                      de_ratio <= 50 and dividendYield >= 3.9 and payoutRatio >= 25:
                        picks_list.append(ticker)
                    # Everywhere else
                    elif eps > 0 and pe <= 15 and pb <= 1.59 and\
                      de_ratio <= 50 and dividendYield >= 3.9 and payoutRatio >= 25 and beta5yMonthly <= 1.2:
                        picks_list.append(ticker)

                    logger.info("Screening - (%s, %s)" % (dbname, ticker))
                except:
                    logger.info("Failed to collect - (%s, %s)" % (dbname, ticker))
                    pass

            if len(picks_list) > 0:
                picks_dic.update({dbname:picks_list})
                logger.info("Screener found - (%s, %s)" % (dbname, picks_list))
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
    return {'6m': round(sixm,2),
            '1y': round(oney,2),
            '5y': round(fivey,2)
           }


def get_keyStat(dbname, ticker):
    t = yf.Ticker(ticker)
    try:
        tinfo = t.info
    except:
        tinfo = None
    try:
        tfinfo = t.fast_info
    except:
        tfinfo = None

    if tinfo is not None and tfinfo is not None:
        try:
            bvps = tinfo['bookValue']
        except:
            bvps = None
        try:
            ps_ratio = round(tinfo['priceToSalesTrailing12Months'],2)
        except:
            ps_ratio = None
        try:
            pb_ratio = round(tinfo['priceToBook'],2)
        except:
            pb_ratio = None
        try:
            eps = tinfo['trailingEps']
        except:
            eps = None
        try:
            if tinfo['currentRatio'] is not None:
                cr = tinfo['currentRatio']
            else:
                cr = 1
        except:
            cr = 1
        try:
            if tinfo['debtToEquity'] is not None:
                de_ratio = tinfo['debtToEquity']
            else:
                de_ratio = 50
        except:
            de_ratio = 50
        try:
            dy = tinfo['dividendYield']
        except:
            dy = None
        try:
            payoutRatio = round(tinfo['payoutRatio']*100,2)
        except:
            payoutRatio = None
        try:
            dividendRate = tinfo['dividendRate']
        except:
            dividendRate = None
        try:
            if tinfo['forwardPE'] != None:
                PE = tinfo['forwardPE']
            else:
                PE = tinfo['trailingPE']
        except:
            PE = None
        try:
            DeltaDR_PE = round((float(dy)*100) - float(forwardPE),2)
        except:
            DeltaDR_PE = None
        #marketCap
        try:
            mc =  tfinfo['market_cap']
        except:
            mc = None
        #Margins
        try:
            ebitdaMargins = round(tinfo['ebitdaMargins']*100,2)
        except:
            ebitdaMargins = None
        try:
            profitMargins = round(tinfo['profitMargins']*100,2)
        except:
            profitMargins = None
        try:
            grossMargins = round(tinfo['grossMargins']*100,2)
        except:
            grossMargins = None
        try:
            operatingMargins = round(tinfo['operatingMargins']*100,2)
        except:
            operatingMargins = None
        #Growth
        try:
            earningsQuarterlyGrowth = round(tinfo['earningsQuarterlyGrowth']*100,2)
        except:
            earningsQuarterlyGrowth = None
        try:
            revenueQuarterlyGrowth = round(tinfo['revenueGrowth']*100,2)
        except:
            revenueQuarterlyGrowth = None
        #Beta
        try:
            beta5yMonthly = tinfo['beta']
        except:
            beta5yMonthly = None

        data = {
                'bvps': bvps,
                'cr': cr,
                'de': de_ratio,
                'ps': ps_ratio,
                'pb': pb_ratio,
                'eps': eps,
                'dy': dy,
                'mc': mc,
                'por':payoutRatio,
                'dr': dividendRate,
                'pe': PE,
                'deltaDR_PE': DeltaDR_PE,
                'ebitdaMargins': ebitdaMargins,
                'profitMargins': profitMargins,
                'grossMargins': grossMargins,
                'operatingMargins': operatingMargins,
                'earningsQuarterlyGrowth': earningsQuarterlyGrowth,
                'revenueQuarterlyGrowth': revenueQuarterlyGrowth,
                'beta5yMonthly':beta5yMonthly
                }

        if dbname != 'csi300':
            # Supplement for Stock Chart Summary
            try:
                scs = stockChartSummary(ticker)
                if data['beta5yMonthly'] is None and scs['Beta60Month'] not in ('','-'):
                    data['beta5yMonthly'] = float(scs['Beta60Month'])
            except:
                pass

        return data


def get_yahoo_finance_price(ticker):
    try:
        t = yf.Ticker(ticker)
        data = t.history(period="1day")
        return round(data.iloc[-1]['Close'],2)
    except:
        return None

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
        return funda
    except:
        pass