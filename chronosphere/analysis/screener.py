import json
import logging
import re
import time

import pandas as pd
import requests
import yfinance as yf
from yahooquery import Ticker as yhqT


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
                        ks = get_keyStat(dbname, ticker + ".TO")
                    elif dbname == 'csi300':
                        ticker = ticker.replace('SH', 'SS')
                        ks = get_keyStat(dbname, ticker)
                    else:
                        ks = get_keyStat(dbname, ticker)

                    # Screener
                    pe = ks['pe']
                    pb = ks['pb']
                    eps = ks['eps']
                    de_ratio = ks['de']
                    ps = ks['ps']
                    dividendYield = ks['dy'] * 100
                    payoutRatio = ks['por']

                    # Screener conditions
                    # China
                    if dbname == 'csi300' and eps > 0 and pe <= 15 and pb <= 1.59 and ps <= 2 and\
                      de_ratio <= 50 and dividendYield >= 4.05 and payoutRatio >= 25:
                        picks_list.append(ticker)
                    # Everywhere else
                    elif eps > 0 and pe <= 15 and pb <= 1.59 and ps <= 2 and\
                      de_ratio <= 50 and dividendYield >= 4.05 and payoutRatio >= 25 and ks['beta'] <= 1.2:
                        picks_list.append(ticker)

                    logger.info("Screening - (%s, %s)" % (dbname, ticker))
                except:
                    logger.info("Failed to collect - (%s, %s)" % (dbname, ticker))
                    pass

            if len(picks_list) > 0:
                picks_dic.update({dbname: picks_list})
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
    return {'6m': round(sixm, 2),
            '1y': round(oney, 2),
            '5y': round(fivey, 2)
           }


def get_keyStat(dbname, ticker):
    data = {}
    t = yf.Ticker(ticker)
    try:
        tinfo = t.info
    except:
        tinfo = None
    try:
        tfinfo = t.fast_info
    except:
        tfinfo = None

    # BVPS
    try:
        data.update({'bvps': tinfo['bookValue']})
    except:
        pass

    # Price/Sales
    try:
        data.update({'ps': round(tinfo['priceToSalesTrailing12Months'], 2)})
    except:
        pass

    # Price/Book
    try:
        data.update({'pb': round(tinfo['priceToBook'], 2)})
    except:
        pass

    # EPS
    try:
        if tinfo['trailingEps'] is None:
            data.update({'eps': tinfo['forwardEps']})
        else:
            data.update({'eps': tinfo['trailingEps']})
    except:
        pass

    # Current Ratio
    try:
        data.update({'cr': tinfo['currentRatio']})
    except:
        data.update({'cr': 1})

    # Debt to Equity
    try:
        data.update({'de': tinfo['debtToEquity']})
    except:
        data.update({'de': 50})

    # Dividend Yield
    try:
        data.update({'dy': tinfo['dividendYield']})
    except:
        data.update({'dy': 0})

    # Payout Ratio
    try:
        data.update({'por': round(tinfo['payoutRatio']*100, 2)})
    except:
        data.update({'por': 0})

    # P/E
    try:
        if tinfo['trailingPE'] is None:
            data.update({'pe': tinfo['forwardPE']})
        else:
            data.update({'pe': tinfo['trailingPE']})
    except:
        pass

    #marketCap
    try:
        data.update({'mc': tfinfo['market_cap']})
    except:
        pass

    #Margins
    try:
        data.update({'ebitdaMargins': round(tinfo['ebitdaMargins']*100, 2)})
    except:
        pass
    try:
        data.update({'profitMargins': round(tinfo['profitMargins']*100, 2)})
    except:
        pass
    try:
        data.update({'grossMargins': round(tinfo['grossMargins']*100, 2)})
    except:
        pass
    try:
        data.update({'operatingMargins': round(tinfo['operatingMargins']*100, 2)})
    except:
        pass
    #Growth
    try:
        data.update({'earningsQuarterlyGrowth': round(tinfo['earningsQuarterlyGrowth']*100, 2)})
    except:
        pass
    try:
        data.update({'revenueQuarterlyGrowth': round(tinfo['revenueGrowth']*100, 2)})
    except:
        pass
    #Beta
    try:
        data.update({'beta': tinfo['beta']})
    except:
        pass

    # Supplement from yahooquery
    try:
        yhq = yhqT(ticker)
        yhqs = yhq.summary_detail[ticker]
    except:
        yhqs = None
    # P/E
    try:
        if yhqs['trailingPE'] is None:
            data.update({'pe': yhqs['forwardPE']})
        else:
            data.update({'pe': yhqs['trailingPE']})
    except:
        pass
    # DR
    try:
        data.update({'dr': yhqs['dividendRate']})
    except:
        pass
    # DY
    try:
        data.update({'dy': yhqs['dividendYield']})
    except:
        pass
    # POR
    try:
        data.update({'por': round(yhqs['payoutRatio'] * 100, 2)})
    except:
        pass
    # Beta
    try:
        data.update({'beta': yhqs['beta']})
    except:
        pass
    # Price/sales
    try:
        data.update({'ps': yhqs['priceToSalesTrailing12Months']})
    except:
        pass

    # Supplement from Stock Chart Summary
    if dbname != 'csi300':
        try:
            scs = stockChartSummary(ticker)
        except:
            scs = None
        # Beta
        try:
            if 'beta' not in data and scs['Beta60Month'] not in ('', '-', '0.00', '0'):
                data.update({'beta': float(scs['Beta60Month'])})
        except:
            pass
        # EPS
        try:
            if 'eps' not in data and scs['EPS'] not in ('', '-'):
                data.update({'eps': float(scs['EPS'])})
        except:
            pass
        # Price/Book
        try:
            if 'pb' not in data and scs['PriceToBook'] not in ('', '-', '0.00', '0'):
                data.update({'pb': float(scs['PriceToBook'])})
        except:
            pass
        try:
            if 'pb' not in data and scs['Price To Book '] not in ('', '-', '0.00', '0'):
                data.update({'pb': float(scs['Price To Book '])})
        except:
            pass
        try:
            if 'pb' not in data and scs['Price To Book'] not in ('', '-', '0.00', '0'):
                data.update({'pb': float(scs['Price To Book'])})
        except:
            pass

    # Supplement from Calculation
        # EPS
        try:
            if 'pe' in data and 'eps' not in data:
                eps = round(get_yahoo_finance_price(ticker) / data['pe'], 2)
                data.update({'eps': eps})
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