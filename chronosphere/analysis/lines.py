import logging
import pandas as pd
from datetime import timedelta
from ..models import Index, Quote, Quote_CSI300, Line_report
from copy import deepcopy
from ..utils.utils import gen_id
logger = logging.getLogger('main.lines')

def line_analysis(sdic):
    # Create Tor_report table
    Line_report.__table__.create(sdic['learning'].get_bind(), checkfirst=True)
    s_l = sdic['learning']
    for dbname, s in sdic.items():
        if dbname in ('tsxci','nasdaq100','sp100','csi300'):
            logger.info("Start to process: %s" % dbname)
            tickers = [r.symbol for r in s.query(Index.symbol).distinct()]
            for ticker in tickers:
            # for ticker in ['F']:
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
                latest_close = df_latest['close'].item()
                latest_date = df_latest.name
                # Highest
                df_max = df.loc[df['close'].idxmax()]
                max_date = df_max.name
                max_price = df_max['close'].item()
                # Lowest
                df_min = df.loc[df['close'].idxmin()]
                min_date = df_min.name
                min_price = df_min['close'].item()
                # EMA
                ema21 = ema(df,21)
                ema5 = ema(df,5)
                delta521 = ema5 - ema21
                # EMA 5 below EMA 21
                df['delta_neg'] = delta521 < 0
                # EMA 5 above EMA 21
                df['delta_pos'] = delta521 > 0
                # 1. H-L-C model
                if (min_date - max_date).days > 0:
                    model = 'hlc'
                    # Lowest price date, index
                    i = df.index.get_loc(min_date)
                    # df range after year low to now
                    df = df.loc[min_date:latest_date]
                # 2. H-L-C model
                elif (min_date - max_date).days < 0:
                    model = 'lhc'
                    # Highest price date, index
                    i = df.index.get_loc(max_date)
                    # df range after year low to now
                    df = df.loc[max_date:latest_date]
                # Support dots:
                support_dots = find_dots(df, 'delta_neg', 'support')
                support_dots = convert_date_index(df, support_dots)
                support_lines = find_lines(df, support_dots, 'support')
                support_lines = check_lines(df, support_lines)
                # Resistance dots:
                resistance_dots = find_dots(df, 'delta_pos', 'resistance')
                resistance_dots = convert_date_index(df, resistance_dots)
                resistance_lines = find_lines(df, resistance_dots, 'resistance')
                resistance_lines = check_lines(df, resistance_lines)
                # Total Lines
                all_lines = refine_lines(df, support_lines, resistance_lines, model)
                if all_lines:
                    try:
                        # Delete all rows of ticker
                        s_l.query(Line_report).filter(Line_report.symbol==ticker, Line_report.index==dbname).delete()
                        s_l.commit()
                        # Update rows
                        for line in all_lines:
                            line.update({'id':gen_id(ticker+dbname+str(latest_date)+str(line['x1'])+str(line['x2'])),
                                        'date':latest_date,
                                        'symbol':ticker,
                                        'index':dbname,
                                        'model':model})
                            del line['line_price_latest']
                            s_l.add(Line_report(**line))
                            s_l.commit()
                        logger.info("Written %s records - (%s, %s)" % (str(len(all_lines)), dbname, ticker))
                    except Exception as e:
                        s_l.rollback()
                        logger.error("%s - (%s, %s)" % (type(e).__name__, dbname, ticker))




def ema(df, span):
    df = df['close'].ewm(span=span, adjust=False).mean()
    return df


def find_dots(df, column_name, condition):
    # Resistance frist dot has to start with highest
    if condition == 'resistance':
        df_resis_max = df.loc[df['close'].idxmax()]
        max_resis_date = df_resis_max.name
        df = df.loc[max_resis_date:]
    temp = []
    final = []
    temp_price = []
    flag = None
    for i, (index, row) in enumerate(df.iterrows()):
        if row[column_name] == True:
            if temp_price:
                if condition == 'support' and row['close'] < min(temp_price):
                    temp_price.append(row['close'])
                    del temp[:]
                    temp.append({'date':index, 'close':row['close']})
                elif condition == 'resistance' and row['close'] > max(temp_price):
                    temp_price.append(row['close'])
                    del temp[:]
                    temp.append({'date':index, 'close':row['close']})
            else:
                temp_price.append(row['close'])
                temp.append({'date':index, 'close':row['close']})
            flag = True
        elif row[column_name] == False:
            if flag == True:
                final += deepcopy(temp)
                del temp[:]
                del temp_price[:]
            flag = False
    return final


def convert_date_index(df, dots):
    for p in dots:
        p['date'] = df.index.get_loc(p['date'])
    return dots


def find_lines(df, dots, type):
    df_latest = df.iloc[-1]
    latest_date = df_latest.name
    latest_dot = df.index.get_loc(latest_date)
    # More than one dot
    if len(dots) > 1:
        final_lines = []
        for i, p1 in enumerate(dots):
            x1 = p1['date']
            y1 = p1['close']
            temp_slope = None
            temp_lines = []
            for p2 in dots[i+1:]:
                x2 = p2['date']
                y2 = p2['close']
                slope = (y2-y1)/(x2-x1)
                if temp_slope != None:
                    if (type == 'support' and slope >= 0 and slope < temp_slope) or (type == 'resistance' and slope <= 0 and slope > temp_slope):
                        del temp_lines[:]
                        temp_slope = slope
                        temp_lines.append({'x1':df.iloc()[x1].name,'x2':df.iloc()[x2].name,'slope':slope,'type':type})
                else:
                    if (type == 'support' and slope >= 0) or (type == 'resistance' and slope <= 0):
                        temp_slope = slope
                        temp_lines.append({'x1':df.iloc()[x1].name,'x2':df.iloc()[x2].name,'slope':slope,'type':type})
            final_lines += deepcopy(temp_lines)
        return final_lines


def check_lines(df, lines):
    if not lines or lines == None:
        return []
    # Latest Close, High, Low
    latest = df.iloc[-1]
    latest_close = latest['close'].item()
    latest_high = latest['high'].item()
    latest_low = latest['low'].item()
    #Previous Close
    previous = df.iloc[-2]
    pre_close = previous['close'].item()
    # Latest Dot
    latest_date = latest.name
    latest_dot = df.index.get_loc(latest_date)
    # Previous Dot
    previous_dot = latest_dot-1
    for line in lines:
        type = line['type']
        x1 = df.index.get_loc(line['x1'])
        y1 = df.iloc()[x1]['close']
        slope = line['slope']
        # line price Now
        line_price_latest = round(slope * (latest_dot-x1)+y1,2)
        # line price Previous
        line_price_pre = round(slope * (previous_dot-x1)+y1,2)
        if type == 'support':
            line.update({'line_price_latest':line_price_latest})
            if (line_price_pre <= pre_close) and (line_price_latest <= latest_close) and (line_price_latest >= latest_low):
                line.update({'touching':True})
            elif (line_price_pre < pre_close) and (line_price_latest > latest_close):
                line.update({'breaking':True})
            elif (line_price_pre > pre_close) and (line_price_latest < latest_close):
                line.update({'reunion':True})
            else:
                line.update({'flag':True})
        elif type == 'resistance':
            line.update({'line_price_latest':line_price_latest})
            if (line_price_pre >= pre_close) and (line_price_latest >= latest_close) and (line_price_latest <= latest_high):
                line.update({'touching':True})
            elif (line_price_pre > pre_close) and (line_price_latest < latest_close):
                line.update({'breaking':True})
            elif (line_price_pre < pre_close) and (line_price_latest > latest_close):
                line.update({'reunion':True})
            else:
                line.update({'flag':True})
    # Remove items that not in filters
    lines = [l for l in lines if 'flag' not in l]
    if lines:
        return lines
    else:
        return []


def extract_value_tolist(diclist, key_name):
    seq = [x[key_name] for x in diclist]
    return seq


def refine_lines(df, support_lines, resistance_lines, model):
    latest = df.iloc[-1]
    latest_close = latest['close'].item()
    _ALL_LINES = support_lines + resistance_lines
    # Line price
    SUPP_PRICE = extract_value_tolist(support_lines, 'line_price_latest')
    RESIS_PRICE = extract_value_tolist(resistance_lines, 'line_price_latest')
    _MIN_SUPP_PRICE = min(SUPP_PRICE, default=[])
    _MAX_SUPP_PRICE = max(RESIS_PRICE, default=[])
    _MIN_RESIS_PRICE = min(RESIS_PRICE, default=[])
    _MAX_RESIS_PRICE = max(RESIS_PRICE, default=[])
    if support_lines and resistance_lines:
        overlap = _MIN_RESIS_PRICE < _MIN_SUPP_PRICE or _MIN_RESIS_PRICE <_MAX_SUPP_PRICE or\
                    _MAX_RESIS_PRICE < _MIN_SUPP_PRICE or _MAX_RESIS_PRICE < _MAX_SUPP_PRICE
        for line in _ALL_LINES:
            type = line['type']
            line_price_latest = line['line_price_latest']
            if type == 'support' and overlap and model == 'lhc':
                _ALL_LINES.remove(line)
            elif type == 'resistance' and overlap and model == 'hlc':
                _ALL_LINES.remove(line)
    return _ALL_LINES
