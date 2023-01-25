import logging
import pandas as pd
from datetime import timedelta
from ..models import Index, Quote, Quote_CSI300, Ublb_cross, Rsi_predict_report
from ..utils.utils import gen_id
from stockstats import StockDataFrame

logger = logging.getLogger('main.ublb')
pd.set_option('mode.chained_assignment', None)

def ublb_cross_analysis(sdic):
    # Create UBLB Cross table
    s_l = sdic['learning']
    Ublb_cross.__table__.create(s_l.get_bind(), checkfirst=True)

    for dbname, s in sdic.items():
        if dbname in ('testing','tsxci','nasdaq100','sp100','csi300','eei','commodity'):
            logger.info("Start to process: %s" % dbname)

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

                reached_date = rsi30_rpr.loc[rsi30_rpr['symbol'] == ticker]['reached_date'].iloc[-1]

                # Latest
                df = df[(df != 0).all(1)]
                df = df.sort_index(ascending=True).last('52w').drop(columns=['id'])
                df = df[-120:]

                latest_date = df.iloc[-1].name
                pre_date = df.iloc[-2].name



                # Bolling band diff
                stock = StockDataFrame.retype(df)
                boll_diff = round(stock['boll'].diff(),2)
                ub_diff = round(stock['boll_ub'].diff(),2)
                lb_diff = round(stock['boll_lb'].diff(),2)

                # Latest/Previous boll Diff info
                latest_boll = boll_diff[-1]
                latest_ub = ub_diff[-1]
                latest_lb = lb_diff[-1]

                pre_boll = boll_diff[-2]
                pre_ub = ub_diff[-2]
                pre_lb = lb_diff[-2]

                # Recent Rsi
                rsi_df = StockDataFrame.retype(df)
                rsi_df['rsi_14'] = rsi_df['rsi_14']
                latest_rsi = rsi_df['rsi_14'].iloc[-1]

                try:
                    # Define pattern ub < lb first time
                    if ((latest_boll < 0 and pre_boll < 0 and
                        latest_ub < 0 and latest_lb <0 and
                        latest_ub < latest_boll and latest_ub < latest_lb and
                        latest_lb > latest_boll and latest_lb > latest_ub) and
                        pre_ub > pre_boll and pre_ub > pre_lb and
                        pre_lb < pre_boll and pre_lb < pre_ub
                       ):
                       # Remove existing record
                       s_l.query(Ublb_cross).filter(Ublb_cross.symbol == ticker,
                                                     Ublb_cross.index == dbname).delete()
                       s_l.commit()
                       record = {'id': gen_id(ticker+dbname+str(reached_date)+str(latest_date)),
                                 'date': latest_date,
                                 'reached_date': reached_date,
                                 'index': dbname,
                                 'symbol': ticker,
                                 }
                       s_l.add(Ublb_cross(**record))
                       s_l.commit()
                       logger.info("Ublb Cross found - (%s, %s)" % (dbname, ticker))
                    # Remove record if rsi at 70+
                    elif latest_rsi >= 70:
                        record = s_l.query(Ublb_cross).filter(Ublb_cross.symbol == ticker,
                                                      Ublb_cross.index == dbname).first()
                        if record:
                            s_l.delete(record)
                            s_l.commit()
                            logger.info("Ublb Cross deleted - (%s, %s)" % (dbname, ticker))
                except:
                    pass
