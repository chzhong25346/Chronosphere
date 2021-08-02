import logging
import pandas as pd
from datetime import timedelta
from ..models import Tor_report, Index, Quote, Quote_CSI300, Report, Shares_outstanding
from ..utils.utils import find_dateByVolume, distance_date, gen_id
logger = logging.getLogger('main.turnover')


def tor_analysis(sdic):
    # Create Tor_report table
    Tor_report.__table__.create(sdic['learning'].get_bind(), checkfirst=True)
    s_l = sdic['learning']
    s_f = sdic['financials']
    for dbname, s in sdic.items():
        if dbname in ('tsxci','nasdaq100','sp100','csi300','eei'):
            logger.info("Start to process: %s" % dbname)
            tickers = [r.symbol for r in s.query(Index.symbol).distinct()]
            for ticker in tickers:
            # for ticker in ['000671.SZ']:
                try:
                    tdic = tor_main(ticker, dbname, s, s_f)
                except:
                    pass
                if tdic:
                    tdic.update({'id':gen_id(ticker+dbname+str(tdic['date'])),
                                'index':dbname})
                    try:
                        s_l.add(Tor_report(**tdic))
                        s_l.commit()
                        logger.info("Written record - (%s, %s)" % (dbname, ticker))
                    except Exception as e:
                        s_l.rollback()
                        logger.error("%s - (%s, %s)" % (type(e).__name__, dbname, ticker))


def tor_main(ticker, dbname, s, s_f):
    # Query Shares Outstanding
    if dbname == 'tsxci':
        shares_os = s_f.query(Shares_outstanding).get(ticker+'.TO')
    else:
        shares_os = s_f.query(Shares_outstanding).get(ticker)
    # Df ticker's report
    report = pd.read_sql(s.query(Report).\
            filter(Report.symbol == ticker).\
            statement, s.bind, index_col='date').sort_index()
    # Df ticker's Quote
    if dbname == 'csi300':
        df = pd.read_sql(s.query(Quote_CSI300).\
            filter(Quote_CSI300.symbol == ticker).\
            statement, s.bind, index_col='date').sort_index()
    else:
        df = pd.read_sql(s.query(Quote).\
            filter(Quote.symbol == ticker).\
            statement, s.bind, index_col='date').sort_index()

    df = df.sort_index(ascending=True).last('52w')

    # All Found:  Shares Outstanding, Report and Quote
    if shares_os and not df.empty and not report.empty:
        shares_os = shares_os.shares
        df_latest = df.iloc[-1]
        latest_close = df_latest['close'].item()
        latest_high = df_latest['high'].item()
        latest_low = df_latest['low'].item()
        latest_date = df_latest.name

        df_max = df.loc[df['high'].idxmax()]
        max_date = df_max.name
        max_price = df_max['close'].item()

        df_min = df.loc[df['low'].idxmin()]
        min_date = df_min.name
        min_price = df_min['close'].item()
        # 1. H-L-C model
        if (min_date - max_date).days > 0:
            # Lowest price date, index
            i = df.index.get_loc(min_date)
            ic = df.index.get_loc(latest_date)
            # Current is at Year High, Stop computing
            if i == ic:
                return None
            # Lowest date + 1
            min_plus1_date = df.iloc()[i+1].name
            # max date between L1 and C
            max_l1_c_date = df.loc[min_plus1_date:latest_date]['close'].idxmax()

            # 2. Find total volume between L1 and Max_L1C
            found_volume = df.loc[min_plus1_date:max_l1_c_date]['volume'].sum()

            # 3. Find Crash date beginning from Lowest date accumated by found volume
            crash_date = find_dateByVolume(df, min_date, found_volume, direction='backward').name

            # 4. Varify and Recalibrate
            # 4.1 Find a new Crash Date back and forth 30 natrual days based on old crash date
            days_30= timedelta(days=30)
            back = report.loc[crash_date - days_30:crash_date]
            forth = report.loc[crash_date:crash_date + days_30]
            candidate_dates = pd.concat([back, forth]).loc[(report['downtrend'] == True)|(report['bolling'] == 'sell')].index

            if not candidate_dates.empty:
                # first crash date
                new_crash_date1 = distance_date(candidate_dates, crash_date, "nearest")
                if new_crash_date1 < min_date:

                    # 4.3 If there is new high between new crash date and min_date
                    # crash_high_date = df.loc[new_crash_date1:min_date]['high'].idxmax()

                    # 4.4 found new crash_date
                    # report2 = report.loc[crash_high_date:min_date]
                    report2 = report.loc[new_crash_date1:min_date]

                    candidate_dates = report2.loc[(report2['downtrend'] == True)|(report2['bolling'] == 'sell')].index
                    # if not candidate_dates.empty:
                    new_crash_date2 = distance_date(candidate_dates, crash_date, "farest")

                    # 4.5 If 1st and 2nd crash date, which one closer to max
                    crash1toMax = abs(df.loc[new_crash_date1]['close']/max_price-1)
                    crash2toMax = abs(df.loc[new_crash_date2]['close']/max_price-1)
                    if crash1toMax < 0.25 and crash2toMax < 0.25:
                        new_crash_date = new_crash_date2
                    elif crash1toMax > 0.25 and crash2toMax > 0.25:
                        return None
                    elif crash1toMax < crash2toMax:
                        new_crash_date = new_crash_date1
                    elif crash1toMax > crash2toMax:
                        new_crash_date = new_crash_date2
                    else:
                        return None

                    # 5. Recalibrate volume between new crash data and lowest date
                    recalibrate_volume = df.loc[new_crash_date:min_date]['volume'].sum()
                    recorded_tor = round(recalibrate_volume/shares_os*100,2)

                    # 5.1 Current volume and Turnover Ratio
                    current_volume = df.loc[min_plus1_date:latest_date]['volume'].sum()
                    current_tor = round(current_volume/shares_os*100,2)

                    # 5.2 Current Volume vs Recalibrate Volume
                    volume_buffer = round(((recalibrate_volume-current_volume)/((recalibrate_volume+current_volume)/2))*100,2)

                    # 6. Find Alert date when volume reached
                    try:
                        alert_date = find_dateByVolume(df, min_plus1_date, recalibrate_volume, direction='forward').name
                    except:
                        alert_date =  None


                    if -5 <= volume_buffer <= 5:
                        return {
                                'symbol':ticker,
                                'date': latest_date,
                                'model':'hlc',
                                'alert_date':alert_date,
                                'recorded_start':new_crash_date,
                                'recorded_end':min_date,
                                'current_start':min_plus1_date,
                                'recorded_tor':recorded_tor,
                                'current_tor':current_tor,
                                'volume_buffer':volume_buffer}

        # 1. L-H-C model
        elif (min_date - max_date).days < 0:
            # Highest price date, index
            i = df.index.get_loc(max_date)
            ic = df.index.get_loc(latest_date)
            # Current is at Year High, Stop computing
            if i == ic:
                return None
            # Highest date + 1
            max_plus1_date = df.iloc()[i+1].name
            # min date between H1 and C
            min_h1_c_date = df.loc[max_plus1_date:latest_date]['close'].idxmin()

            # 2. Find total volume between H1 and Min_H1C
            found_volume = df.loc[max_plus1_date:min_h1_c_date]['volume'].sum()

            # 3. Find breakthrough date beginning from Highest date accumated by found volume
            bt_date = find_dateByVolume(df, max_date, found_volume, direction='backward').name

            # 4. Varify and Recalibrate
            # 4.1 Find a new Crash Date back and forth 30 natrual days based on old crash date
            days_30= timedelta(days=30)
            back = report.loc[bt_date - days_30:bt_date]
            forth = report.loc[bt_date:bt_date + days_30]
            candidate_dates = pd.concat([back, forth]).loc[(report['uptrend'] == True)|(report['bolling'] == 'buy')].index
            # Found Breakthrough date Candidates
            if not candidate_dates.empty:
                # 4.2 Filter: Date > Year Low date and pick smallest, get new breakthrough date
                candidate_dates = [i for i in candidate_dates if i > min_date]
                if len(candidate_dates) > 0:
                    new_bt_date1 = distance_date(candidate_dates, min_date, "nearest")

                    # 4.3 is there antheor new breakthrough date between year low and new bt date 1
                    # If there is new low between new bt date and high date
                    bt_low_date = df.loc[new_bt_date1:max_date]['low'].idxmin()

                    report2 = report.loc[min_date:bt_low_date]
                    candidate_dates = report2.loc[(report2['uptrend'] == True)|(report2['bolling'] == 'buy')].index
                    new_bt_date2 = distance_date(candidate_dates, min_date, "nearest")

                    # 4.4 decide final bt date
                    bt1toMin = abs(df.loc[new_bt_date1]['close']/min_price-1)
                    bt2toMin = abs(df.loc[new_bt_date2]['close']/min_price-1)

                    if new_bt_date1 == new_bt_date2:
                        new_bt_date = new_bt_date1
                    if bt1toMin < 0.25 and bt2toMin < 0.25:
                        new_bt_date = new_bt_date2
                    elif bt1toMin > 0.25 and bt2toMin > 0.25:
                        return None
                    elif bt1toMin < bt2toMin:
                        new_bt_date = new_bt_date1
                    elif bt1toMin > bt2toMin:
                        new_bt_date = new_bt_date2
                    else:
                        return None

                    # 5. Recalibrate volume between new crash data and lowest date
                    recalibrate_volume = df.loc[new_bt_date:max_date]['volume'].sum()
                    recorded_tor = round(recalibrate_volume/shares_os*100,2)

                    # 5.1 Current volume and Turnover Ratio
                    current_volume = df.loc[max_plus1_date:latest_date]['volume'].sum()
                    current_tor = round(current_volume/shares_os*100,2)

                    # 5.2 Current Volume vs Recalibrate Volume
                    volume_buffer = round(((recalibrate_volume-current_volume)/((recalibrate_volume+current_volume)/2))*100,2)

                    # 6. Find Alert date when volume reached
                    try:
                        alert_date = find_dateByVolume(df, max_plus1_date, recalibrate_volume, direction='forward').name
                    except:
                        alert_date = None

                    if -5 <= volume_buffer <= 5:
                        return {'symbol':ticker,
                                'date': latest_date,
                                'model':'lhc',
                                'alert_date':alert_date,
                                'recorded_start':new_bt_date,
                                'recorded_end':max_date,
                                'current_start':max_plus1_date,
                                'recorded_tor':recorded_tor,
                                'current_tor':current_tor,
                                'volume_buffer':volume_buffer}
