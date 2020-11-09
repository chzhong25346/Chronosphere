import logging
import pandas as pd
from datetime import timedelta
from ..models import Index, Quote, Quote_CSI300, Gaps
from ..utils.utils import gen_id
logger = logging.getLogger('main.gaps')

def gap_analysis(sdic):
    # Create Tor_report table
    Gaps.__table__.create(sdic['learning'].get_bind(), checkfirst=True)
    s_l = sdic['learning']
    for dbname, s in sdic.items():
        if dbname in ('tsxci','nasdaq100','sp100','csi300'):
            logger.info("Start to process: %s" % dbname)
            tickers = [r.symbol for r in s.query(Index.symbol).distinct()]
            for ticker in tickers:
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
                        logger.info("Written %s records - (%s, %s)" % (count, dbname, ticker))
