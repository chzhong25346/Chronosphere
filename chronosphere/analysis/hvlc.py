import logging
import pandas as pd
from datetime import timedelta, date
from ..models import Index, Quote, Quote_CSI300, Hvlc_report, Ublb_cross,\
Hvlc_strategy, Hvlc_report_history
from ..utils.utils import gen_id, latest_over_rsi70
logger = logging.getLogger('main.hvlc')

def hvlc_report(sdic):
    # Create Hvlc_report table
    s_l = sdic['learning']
    Hvlc_report.__table__.create(s_l.get_bind(), checkfirst=True)
    Hvlc_strategy.__table__.create(s_l.get_bind(), checkfirst=True)
    Hvlc_report_history.__table__.create(s_l.get_bind(), checkfirst=True)

    for dbname, s in sdic.items():
        if dbname in ('testing','tsxci','nasdaq100','sp100','csi300','eei'):
            logger.info("Start to process: %s" % dbname)
            # tickers = [r.symbol for r in s.query(Index.symbol).distinct()]

            # Get UBLB report
            ublb_cross = pd.read_sql(s_l.query(Ublb_cross).
                    filter(Ublb_cross.index == dbname).statement, s_l.bind)
            ublb_tickers = ublb_cross['symbol'].tolist()

            # Get existing HVLC tickers list:
            hvlc_r = pd.read_sql(s_l.query(Hvlc_report).
                    filter(Hvlc_report.index == dbname).statement, s_l.bind)
            hvlc_tickers = hvlc_r['symbol'].tolist()

            # Get Hvlc strategy
            hvlc_strategy = s_l.query(Hvlc_strategy).one()
            vp_ratio_low = hvlc_strategy.vp_ratio_low
            vp_ratio_high = hvlc_strategy.vp_ratio_high
            vr_low = hvlc_strategy.vr_low
            vr_high = hvlc_strategy.vr_high
            pr_low = hvlc_strategy.pr_low
            pr_high = hvlc_strategy.pr_high

            # ---- HVLC report -------Iterate UBLB cross tickers----------------
            for ticker in ublb_tickers:
                if dbname == 'csi300':
                    df = pd.read_sql(s.query(Quote_CSI300).\
                        filter(Quote_CSI300.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()
                else:
                    df = pd.read_sql(s.query(Quote).\
                        filter(Quote.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()
                try:
                    # Latest
                    df = df[(df != 0).all(1)]
                    df = df.sort_index(ascending=True).last('2y').drop(columns=['id'])

                    # find reached date in ublb cross
                    reached_date = ublb_cross.loc[ublb_cross['symbol'] == ticker]['date'].iloc[-1]

                    # Latest day info
                    df_latest = df.iloc[-1]
                    latest_date = df_latest.name
                    latest_close = df_latest['close']
                    latest_open = df_latest['open']
                    latest_high = df_latest['high']
                    latest_low = df_latest['low']

                    over_rsi70 = latest_over_rsi70(df)

                    # avg_vol = average_volume(df,90)

                    # Volume and Price change in percentage
                    # df['volchg'] = round((df['volume']/avg_vol -1)*100,2)
                    # df['pricechg'] = round(abs((df['close']/df['open'] -1)*100),2)

                    # No price change, reset to 0.01 in order to silence noise
                    # df.loc[df['pricechg'] == 0, 'pricechg'] = 0.01
                    # df['vol_price_ratio'] = round(df['volchg']/df['pricechg'],2)

                    vp_rank_ratio, v_rank, p_rank = _get_vp_rank(df, 99)

                    # Slice range after reached date
                    df_after_reached = df.loc[reached_date:]
                    high_date = df_after_reached['high'].idxmax()
                    low_date = df_after_reached['low'].idxmin()
                    high_price = df_after_reached['high'].max()
                    low_price = df_after_reached['low'].min()
                    lowest_close_date = df_after_reached['close'].idxmin()
                    # Latest v/p ratio
                    # latest_vol_price_ratio = df_after_reached.iloc[-1]['vol_price_ratio']

                    # Today can't be highest/lowest and lowest close date in reached range and should be in range of strategy
                    if (latest_close < high_price and latest_close > low_price
                        and latest_date > low_date and latest_date > high_date
                        and latest_date > lowest_close_date and over_rsi70 != True
                        # HVLC Strategy in Learning db
                        and ((vp_ratio_low <= vp_rank_ratio <= vp_ratio_high and vr_low <= v_rank <= vr_high)
                         or (vr_low <= v_rank <= vr_high and pr_low <= p_rank <= pr_high))
                        ):

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
                                      'volchg': v_rank,
                                      'pricechg' : p_rank,
                                      'vol_price_ratio' : vp_rank_ratio
                                      }
                            s_l.add(Hvlc_report(**record))
                            s_l.commit()
                            logger.info("HVLC found - (%s, %s)" % (dbname, ticker))
                        except:
                                pass
                except:
                    pass

            # -------------------- HVLC remove ----------------------------------
            for ticker in hvlc_tickers:
                if dbname == 'csi300':
                    df = pd.read_sql(s.query(Quote_CSI300).\
                        filter(Quote_CSI300.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()
                else:
                    df = pd.read_sql(s.query(Quote).\
                        filter(Quote.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()

                # quote df
                df = df[(df != 0).all(1)]
                df = df.sort_index(ascending=True).last('52w').drop(columns=['id'])

                over_rsi70 = latest_over_rsi70(df)

                # Remove
                hvlc_r = s_l.query(Hvlc_report).filter(Hvlc_report.symbol == ticker,
                                              Hvlc_report.index == dbname).first()
                df_latest = df.iloc[-1]
                latest_close = df_latest['close'].item()
                hvlc_close = df.loc[hvlc_r.date]['close']
                chg_since_hvlc = round((latest_close/hvlc_close-1)*100,2)

                # Clean up if rsi >= 70 in recent min_period
                if over_rsi70 == True:
                    rsi_now = 70
                    # HVLC record
                    record = s_l.query(Hvlc_report).filter(Hvlc_report.symbol == ticker,
                                                  Hvlc_report.index == dbname).first()
                    # HVLC history record
                    his_record = {'id': gen_id(record.symbol+record.index+str(date.today())),
                              'index': record.symbol,
                              'symbol': record.index,
                              'delete_date':date.today(),
                              'hvlc_date':record.date,
                              'reached_date':record.reached_date,
                              'volchg':record.volchg,
                              'pricechg':record.pricechg,
                              'vol_price_ratio':record.vol_price_ratio,
                              'record_rsi':rsi_now}
                elif chg_since_hvlc <= -10:
                    rsi_now = 30
                    # HVLC record
                    record = s_l.query(Hvlc_report).filter(Hvlc_report.symbol == ticker,
                                                  Hvlc_report.index == dbname).first()
                    # HVLC history record
                    his_record = {'id': gen_id(record.symbol+record.index+str(date.today())),
                              'index': record.symbol,
                              'symbol': record.index,
                              'delete_date':date.today(),
                              'hvlc_date':record.date,
                              'reached_date':record.reached_date,
                              'volchg':record.volchg,
                              'pricechg':record.pricechg,
                              'vol_price_ratio':record.vol_price_ratio,
                              'record_rsi':rsi_now}
                try:
                    if record:
                        s_l.delete(record)
                        s_l.commit()
                        logger.info("HVLC deleted - (%s, %s)" % (dbname, ticker))
                        s_l.add(Hvlc_report_history(**his_record))
                        s_l.commit()
                        logger.info("HVLC history added - (%s, %s)" % (dbname, ticker))
                except:
                    pass


def average_volume(df, span):
    avg_vol = df.sort_index(ascending=True).last("6M")
    avg_vol = df['volume'].rolling(window=span).mean()
    return avg_vol


# Calculate ranking of volume and price and volume rank / price rank
def _get_vp_rank(df, n_days):
    try:
        # Calculate standardlize volume
        df['volume_rank'] = df['volume'].rolling(n_days + 1).apply(lambda x: pd.Series(x).rank().iloc[-1], raw=True)
        df['pricechg'] = round(abs((df['close']/df['open'] -1)*100),2)
        df['pricechg_rank'] = df['pricechg'].rolling(n_days + 1).apply(lambda x: pd.Series(x).rank().iloc[-1], raw=True)
        df['vp_rank_ratio'] = round(df['volume_rank']/df['pricechg_rank'],2)
        v_rank = df.iloc[-1]['volume_rank']
        p_rank = df.iloc[-1]['pricechg_rank']
        vp_rank_ratio = df.iloc[-1]['vp_rank_ratio']
        return vp_rank_ratio, v_rank, p_rank
    except:
        pass
