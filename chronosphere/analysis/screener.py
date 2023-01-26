import logging

import pandas as pd
import yfinance as yf

from .email import sendMail
from ..models import Index
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
            # for ticker in ['001227.SZ']:
                try:
                    # Key Stats and Current Price
                    if dbname == "tsxci":
                        keyStat = get_yahoo_keyStat(ticker+".TO")
                        # current_price = get_yahoo_finance_price(ticker+".TO")
                    elif dbname == 'csi300':
                        ticker = ticker.replace('SH', 'SS')
                        keyStat = get_yahoo_keyStat(ticker)
                    else:
                        keyStat = get_yahoo_keyStat(ticker)
                        # current_price = get_yahoo_finance_price(ticker)

                    # Screener
                    ps_ratio = keyStat['ps']
                    pe = keyStat['pe']
                    pb = keyStat['pb']
                    ps = keyStat['ps']
                    eps = keyStat['eps']
                    PExPB = pe * pb
                    de_ratio = keyStat['de']
                    dividendYield = keyStat['dy']*100
                    payoutRatio = keyStat['por']
                    beta5yMonthly = keyStat['beta5yMonthly']
                    marketCap = keyStat['mc']

                    # print(dbname, eps,pe,pb,de_ratio,dividendYield,payoutRatio,beta5yMonthly,marketCap)

                    # Screener conditions
                    # China
                    if dbname == 'csi300' and eps > 0 and pe <= 15 and pb <= 1.5 and PExPB <= 22.5 and\
                      de_ratio <= 50 and dividendYield >= 4 and payoutRatio >= 25:
                        picks_list.append(ticker)
                    # Everywhere else
                    elif eps > 0 and pe <= 15 and pb <= 1.5 and PExPB <= 22.5 and\
                      de_ratio <= 50 and dividendYield >= 4 and payoutRatio >= 25 and beta5yMonthly <= 1:
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


def get_yahoo_keyStat(ticker):
    try:
        t = yf.Ticker(ticker)
        tinfo= t.info
        tfinfo = t.fast_info
        bvps = tinfo['bookValue']
        ps_ratio = round(tinfo['priceToSalesTrailing12Months'],2)
        pb_ratio = round(tinfo['priceToBook'],2)
        eps =tinfo['trailingEps']
        if tinfo['currentRatio'] == None:
            cr = 1
        else:
            cr = tinfo['currentRatio']

        if tinfo['debtToEquity'] == None:
            de_ratio = 50
        else:
            de_ratio = tinfo['debtToEquity']

        if tinfo['dividendYield'] == None:
            dy = 0
        else:
            dy = tinfo['dividendYield']
        try:
            payoutRatio = round(tinfo['payoutRatio']*100,2)
        except:
            payoutRatio = 'NA'
        try:
            dividendRate = tinfo['dividendRate']
        except:
            dividendRate = 'NA'
        try:
            if tinfo['forwardPE'] != None:
                PE = tinfo['forwardPE']
            else:
                PE = tinfo['trailingPE']
        except:
            PE = 'NA'
        try:
            DeltaDR_PE = round((float(dy)*100) - float(forwardPE),2)
        except:
            DeltaDR_PE = 'NA'
        #marketCap
        try:
            mc =  tfinfo['marketCap']
        except:
            mc = 'NA'
        #Margins
        try:
            ebitdaMargins = round(tinfo['ebitdaMargins']*100,2)
        except:
            ebitdaMargins = 'NA'
        try:
            profitMargins = round(tinfo['profitMargins']*100,2)
        except:
            profitMargins = 'NA'
        try:
            grossMargins = round(tinfo['grossMargins']*100,2)
        except:
            grossMargins = 'NA'
        try:
            operatingMargins = round(tinfo['operatingMargins']*100,2)
        except:
            operatingMargins = 'NA'
        #Growth
        try:
            earningsQuarterlyGrowth = round(tinfo['earningsQuarterlyGrowth']*100,2)
        except:
            earningsQuarterlyGrowth = 'NA'
        try:
            revenueQuarterlyGrowth = round(tinfo['revenueGrowth']*100,2)
        except:
            revenueQuarterlyGrowth = 'NA'
        #Beta
        try:
            beta5yMonthly = tinfo['beta']
        except:
            beta5yMonthly = 1

        data = {
                'bvps': float(bvps),
                'cr': float(cr),
                'de': float(de_ratio),
                'ps': float(ps_ratio),
                'pb': float(pb_ratio),
                'eps': float(eps),
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
        return data

    except:
        return None


def get_yahoo_finance_price(ticker):
    try:
        t = yf.Ticker(ticker)
        data = t.history(period="1day")
        return round(data.iloc[-1]['Close'],2)
    except:
        return None
