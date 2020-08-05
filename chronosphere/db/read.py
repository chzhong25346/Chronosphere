from ..models import Index#, Quote
from sqlalchemy import exists
import datetime as dt
import logging
import pandas as pd
logger = logging.getLogger('main.read')

def read_ticker(s):
    list = [obj.symbol for obj in s.query(Index)]
    return list


# def read_exist(s, ticker):
#     date = dt.datetime.today().strftime("%Y-%m-%d")
#     ret = s.query(exists().where(Quote.symbol==ticker).where(Quote.date==date)).scalar()
#     return ret


def has_index(s):
    return s.query(Index).first()


def pd_read_table(tname,engine,index_name):
    try:
        df = pd.read_sql_table(tname,engine,index_col=index_name,coerce_float=False)
        return df
    except Exception as e:
        logger.debug('Cannot read [%s] table! - continue', tname)
        return False
