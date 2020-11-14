import logging
import pandas as pd
from datetime import timedelta
from ..models import Index, Quote, Quote_CSI300, Gaps, Gaps_report
from ..utils.utils import gen_id
logger = logging.getLogger('main.gaps')

def gap_analysis(sdic):
    # Create Tor_report table
    s_l = sdic['learning']
    Gaps.__table__.create(s_l.get_bind(), checkfirst=True)
    Gaps_report.__table__.create(s_l.get_bind(), checkfirst=True)

    for dbname, s in sdic.items():
        if dbname in ('tsxci','nasdaq100','sp100','csi300'):
            logger.info("Start to process: %s" % dbname)
            tickers = [r.symbol for r in s.query(Index.symbol).distinct()]
            for ticker in tickers:
            # for ticker in ['TOU']:
                if dbname == 'csi300':
                    df = pd.read_sql(s.query(Quote_CSI300).\
                        filter(Quote_CSI300.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()
                else:
                    df = pd.read_sql(s.query(Quote).\
                        filter(Quote.symbol == ticker).\
                        statement, s.bind, index_col='date').sort_index()
                df = df.sort_index(ascending=True).last('52w')
                # Latest
                df_latest = df.iloc[-1]
                df['gap_up'] = 0
                df['gap_down'] = 0
                df['gap'] = 0
                df['gap_low'] = None
                df['gap_high'] = None
                latest_close = df_latest['close'].item()
                latest_open = df_latest['open'].item()
                latest_high = df_latest['high'].item()
                latest_low = df_latest['low'].item()
                latest_date = df_latest.name

                # Finding the gaps - Learning.Gaps
                df.loc[df['low'] > df['high'].shift(1), ['gap_up', 'gap']] = 1
                df.loc[df['high'] < df['low'].shift(1), ['gap_down', 'gap']] = 1
                df.loc[df['gap_up'] == 1, 'gap_low'] = df['high'].shift(1)
                df.loc[df['gap_up'] == 1, 'gap_high'] = df['low']
                df.loc[df['gap_down'] == 1, 'gap_low'] = df['high']
                df.loc[df['gap_down'] == 1, 'gap_high'] = df['low'].shift(1)

                gaps = df.loc[df['gap'] == 1][['symbol','gap_high','gap_low']]
                gaps = gaps.reset_index().to_dict("records")
                if gaps:
                    count = 0
                    for gap in gaps:
                        try:
                            gap.update({'id':gen_id(ticker+dbname+str(gap['date'])),
                                        'date':gap['date'],
                                        'index':dbname})
                            s_l.add(Gaps(**gap))
                            s_l.commit()
                            count += 1
                        except:
                            s_l.rollback()
                            pass
                    if count > 0:
                        logger.info("Found %s gaps - (%s, %s)" % (count, dbname, ticker))

                # Gap Touched - Learning.Gaps_report
                df_gaps = pd.read_sql(s_l.query(Gaps).\
                    filter(Gaps.symbol == ticker).\
                    statement, s_l.bind, index_col='date').sort_index()
                df_gaps['touch'] = 0
                df_gaps.loc[((latest_low <= df_gaps['gap_high']) & (latest_low >= df_gaps['gap_low'])) |
                        ((latest_high <= df_gaps['gap_high']) & (latest_high >= df_gaps['gap_low'])) |
                        ((latest_open <= df_gaps['gap_high']) & (latest_open >= df_gaps['gap_low'])) |
                        ((latest_close <= df_gaps['gap_high']) & (latest_close >= df_gaps['gap_low'])) ,
                        'touch'] = 1
                df_gaps = df_gaps.loc[df_gaps['touch'] == 1]
                df_gaps = df_gaps.loc[~df_gaps.index.isin([latest_date])]

                # If touched gaps
                if not df_gaps.dropna().empty:
                    df_gaps.index.rename('gap_date', inplace=True)
                    df_gaps.reset_index(inplace=True)
                    df_gaps.drop(columns=['id','touch','gap_high','gap_low'], inplace=True)
                    df_gaps['date'] = latest_date
                    gaps_report = df_gaps.to_dict("records")
                    # Remove existing Gaps report
                    s_l.query(Gaps_report).filter(Gaps_report.symbol==ticker, Gaps_report.index==dbname).delete()
                    s_l.commit()
                    # Write touched gaps to report
                    count = 0
                    for r in gaps_report:
                        try:
                            r.update({'id':gen_id(r['symbol']+r['index']+str(r['date'])+str(r['gap_date']))})
                            s_l.add(Gaps_report(**r))
                            s_l.commit()
                            count += 1
                        except:
                            s_l.rollback()
                            pass
                    if count > 0:
                        logger.info("Touched %s gaps - (%s, %s)" % (count, dbname, ticker))
