import logging
import pandas as pd
from datetime import timedelta
from ..models import Index, Quote, Quote_CSI300, Hvlc_report, Rsi_predict_report
from ..utils.utils import gen_id
from stockstats import StockDataFrame
logger = logging.getLogger('main.hvlc')

def hvlc_report(sdic):
    # Create Hvlc_report table
    s_l = sdic['learning']
    Hvlc_report.__table__.create(s_l.get_bind(), checkfirst=True)

    for dbname, s in sdic.items():
        if dbname in ('testing','tsxci','nasdaq100','sp100','csi300','eei'):
            logger.info("Start to process: %s" % dbname)
            # tickers = [r.symbol for r in s.query(Index.symbol).distinct()]

            # Get Rsi_predict_report
            rsi30_rpr = pd.read_sql(s_l.query(Rsi_predict_report).
                    filter(Rsi_predict_report.index == dbname,
                    Rsi_predict_report.target_rsi <=31).statement, s_l.bind)
            tickers = rsi30_rpr['symbol'].tolist()

            for ticker in tickers:
                if dbname == 'csi300':
                    df = pd.read_sql(s.query(Quote_CSI300).\
                        filter(Quote_CSI300.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()
                else:
                    df = pd.read_sql(s.query(Quote).\
                        filter(Quote.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()

                # Latest
                df = df[(df != 0).all(1)]
                df = df.sort_index(ascending=True).last('52w').drop(columns=['id'])
                reached_date = rsi30_rpr.loc[rsi30_rpr['symbol'] == ticker]['reached_date'].iloc[-1]

                # Latest day info
                df_latest = df.iloc[-1]
                latest_date = df_latest.name
                latest_close = df_latest['close']
                latest_open = df_latest['open']
                latest_high = df_latest['high']
                latest_low = df_latest['low']

                # latest RSI >= 70 check
                pd.set_option('mode.chained_assignment',None)
                # Calculate RSI-14
                df = StockDataFrame.retype(df)
                df['rsi_14'] = df['rsi_14']
                # DF clearning
                df = df[(df != 0).all(1)]
                df.dropna(inplace=True)
                over_rsi70 = False
                for index, row in df[::-1].iterrows():
                    if row['rsi_14'] >= 70:
                        over_rsi70 = True
                        break
                    elif row['rsi_14'] <= 30:
                        over_rsi70 = False
                        break

                avg_vol = average_volume(df,90)

                # Volume and Price change in percentage
                df['volchg'] = round((df['volume']/avg_vol -1)*100,2)
                df['pricechg'] = round(abs((df['close']/df['open'] -1)*100),2)

                # No price change, reset to 0.01 in order to silence noise
                df.loc[df['pricechg'] == 0, 'pricechg'] = 0.01
                df['vol_price_ratio'] = round(df['volchg']/df['pricechg'],2)

                # Slice range after reached date
                df_after_reached = df.loc[reached_date:]
                high_date = df_after_reached['high'].idxmax()
                low_date = df_after_reached['low'].idxmin()
                high_price = df_after_reached['high'].max()
                low_price = df_after_reached['low'].min()
                lowest_close_date = df_after_reached['close'].idxmin()

                # Latest v/p ratio
                latest_vol_price_ratio = df_after_reached.iloc[-1]['vol_price_ratio']

                # Today can't be highest/lowest and lowest close date in reached range
                if (latest_close < high_price and latest_close > low_price
                    and latest_date > low_date and latest_date > high_date
                    and latest_date > lowest_close_date and latest_vol_price_ratio > 100
                    and over_rsi70 != True):
                    hvlc_date = df_after_reached.iloc[-1]
                    try:
                        # Remove existing record
                        s_l.query(Hvlc_report).filter(Hvlc_report.symbol == ticker,
                                                      Hvlc_report.index == dbname).delete()
                        s_l.commit()
                        record = {'id': gen_id(ticker+dbname+str(reached_date)+str(latest_date)),
                                  'date': latest_date,
                                  'reached_date': reached_date,
                                  'index': dbname,
                                  'symbol': ticker,
                                  'volchg': hvlc_date['volchg'],
                                  'pricechg' : hvlc_date['pricechg'],
                                  'vol_price_ratio' : hvlc_date['vol_price_ratio']
                                  }
                        s_l.add(Hvlc_report(**record))
                        s_l.commit()
                        logger.info("HVLC found - (%s, %s)" % (dbname, ticker))
                    except:
                        pass



def average_volume(df, span):
    avg_vol = df.sort_index(ascending=True).last("6M")
    avg_vol = df['volume'].rolling(window=span).mean()
    return avg_vol
