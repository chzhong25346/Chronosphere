import logging
import pandas as pd
from datetime import timedelta
from ..models import Index, Quote, Quote_CSI300, Rsi_predict
from ..utils.utils import gen_id
from stockstats import StockDataFrame
logger = logging.getLogger('main.gaps')

def rsi_prediction(sdic):
    pd.set_option('mode.chained_assignment',None)

    # Create RSi_predict table
    s_l = sdic['learning']
    Rsi_predict.__table__.create(s_l.get_bind(), checkfirst=True)

    for dbname, s in sdic.items():
        if dbname in ('testing','tsxci','nasdaq100','sp100','csi300'):
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
                # Latest
                df = df[(df != 0).all(1)]
                df = df.sort_index(ascending=True).last('52w').drop(columns=['id'])
                df_latest = df.iloc[-1]
                latest_date = df_latest.name
                latest_close = df_latest['close']
                # Calculate RSI-14
                df = StockDataFrame.retype(df)
                df['rsi_14'] = df['rsi_14']
                # DF clearning
                df = df[(df != 0).all(1)]
                df.dropna(inplace=True)
                # RSI 30, 70 distinguish
                df['rsi_30'] = False
                df['rsi_70'] = False
                df.loc[df['rsi_14'] <= 30, 'rsi_30'] = True
                df.loc[df['rsi_14'] >= 70, 'rsi_70'] = True
                # Last and Previous RSI Position
                previous_rsi = df.iloc[-2]['rsi_14']
                latest_rsi = df.iloc[-1]['rsi_14']
                lastOverBS = df.loc[((df['rsi_30']==True) | (df['rsi_70']==True))].iloc[-1]
                over_buy = lastOverBS['rsi_70']
                over_sell = lastOverBS['rsi_30']
                new_close, new_rsi = None, None
                if over_buy and previous_rsi >= 50 and latest_rsi < 50:
                    new_close, new_rsi = _get_predicted_rsi(df, latest_rsi, 30)
                elif over_buy and previous_rsi >= 70 and latest_rsi < 70:
                    new_close, new_rsi = _get_predicted_rsi(df, latest_rsi, 50)
                elif over_sell and previous_rsi <= 30 and latest_rsi >30:
                    new_close, new_rsi = _get_predicted_rsi(df, latest_rsi, 50)
                elif over_sell and previous_rsi <= 50 and latest_rsi >50:
                    new_close, new_rsi = _get_predicted_rsi(df, latest_rsi, 70)

                if new_close and new_rsi:
                    try:
                        # Remove existing prediction
                        s_l.query(Rsi_predict).filter(Rsi_predict.symbol==ticker, Rsi_predict.index==dbname).delete()
                        s_l.commit()
                        record = {'id':gen_id(ticker+dbname+str(latest_date)),
                                 'date': latest_date,
                                 'index': dbname,
                                 'symbol': ticker,
                                 'current_rsi': round(latest_rsi, 2),
                                 'target_rsi': new_rsi,
                                 'target_close': new_close,
                                 'trend': 'up' if new_rsi > latest_rsi else 'down'
                                }
                        s_l.add(Rsi_predict(**record))
                        s_l.commit()
                        logger.info("Prediction made - (%s, %s)" % (dbname, ticker))
                    except:
                        pass


def _get_rsi(df):
    avg_gain = df['gain'].ewm(ignore_na=False, alpha=1.0 / 14, min_periods=0, adjust=True).mean()[-1]
    avg_loss = df['loss'].ewm(ignore_na=False, alpha=1.0 / 14, min_periods=0, adjust=True).mean()[-1]
    rs = avg_gain/avg_loss
    rsi = 100 - (100/(1+rs))
    return rsi


def _refine_close_price(df, target_rsi, new_value, field):
    df.iloc[-1, df.columns.get_loc(field)] = new_value
    new_rsi = _get_rsi(df)
    temp_new_value = new_value
    # for higher target RSI, new loss is 0, adjust gain
    if field == 'gain':
        # If calculated RSI happens to be larger than target_rsi - reduce gain
        while new_rsi > target_rsi:
            temp_new_value -=  0.01
            df.iloc[-1, df.columns.get_loc('gain')] = temp_new_value
            new_rsi = _get_rsi(df)
        # If calculated RSI happens to be lesser than target_rsi - increase gain
        while new_rsi < target_rsi:
            temp_new_value +=  0.01
            df.iloc[-1, df.columns.get_loc('gain')] = temp_new_value
            new_rsi = _get_rsi(df)
        return temp_new_value, new_rsi
    # for lower target RSI, new gain is 0, adjust loss
    elif field == 'loss':
        # If calculated RSI happens to be larger than target_rsi - increase loss
        while new_rsi > target_rsi:
            temp_new_value +=  0.01
            df.iloc[-1, df.columns.get_loc('loss')] = temp_new_value
            new_rsi = _get_rsi(df)
        # If calculated RSI happens to be lesser than target_rsi - reduce loss
        while new_rsi < target_rsi:
            temp_new_value -=  0.01
            df.iloc[-1, df.columns.get_loc('loss')] = temp_new_value
            new_rsi = _get_rsi(df)
        return temp_new_value, new_rsi


def _get_predicted_rsi(df, latest_rsi, target_rsi):
    # Calculate current RSI
    df_latest = df.iloc[-1]
    latest_date = df_latest.name
    latest_close = df_latest['close']
    df['gain'] = 0
    df['loss'] = 0
    df['change'] = df['close'].diff()
    df.loc[df['change'] > 0, 'gain'] = df['change']
    df.loc[df['change'] < 0, 'loss'] = abs(df['change'])
    # Predict future rsi
    new_day = latest_date + timedelta(days=1)
    target_rs = target_rsi/(100-target_rsi)
    # Gain
    if target_rsi > latest_rsi:
        row = pd.Series({'loss':0}, name=new_day)
        df = df.append(row)
        new_avg_loss = df['loss'].ewm(ignore_na=False, alpha=1.0 / 14, min_periods=0, adjust=True).mean()[-1]
        sum_pre_avg_gain = df['gain'].iloc[-14:].sum()
        sum_avg_gain = target_rs * new_avg_loss * 14
        new_gain = sum_avg_gain - sum_pre_avg_gain
        # Refine
        new_gain, new_rsi = _refine_close_price(df, target_rsi, new_gain, 'gain')
        new_close = round(latest_close + new_gain, 2)
        new_rsi = round(new_rsi, 2)
        return new_close, new_rsi
    # Loss
    elif target_rsi < latest_rsi:
        row = pd.Series({'gain':0}, name=new_day)
        df = df.append(row)
        new_avg_gain = df['gain'].ewm(ignore_na=False, alpha=1.0 / 14, min_periods=0, adjust=True).mean()[-1]
        sum_pre_avg_loss = df['loss'].iloc[-14:].sum()
        sum_avg_loss = new_avg_gain / target_rs * 14
        new_loss = sum_avg_loss - sum_pre_avg_loss
        # Refine
        new_loss, new_rsi = _refine_close_price(df, target_rsi, new_loss, 'loss')
        new_close = round(latest_close - new_loss, 2)
        new_rsi = round(new_rsi, 2)
        return new_close, new_rsi
