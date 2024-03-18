# -*- coding: utf-8 -*-
from .db.db import Db as db


class Index(db.Model):
    __tablename__ = 'index'
    symbol = db.Column(db.String(6), unique=True, nullable=False, primary_key=True)
    company = db.Column(db.String(60),nullable=False)
    # sector = db.Column(db.String(80),nullable=False)
    # industry = db.Column(db.String(60),nullable=False)
    quote = db.relationship('Quote', backref='quote', lazy=True)
    report = db.relationship('Report', backref='report', lazy=True)

class Watchlist_Index(db.Model):
    __tablename__ = 'watchlist'
    symbol = db.Column(db.String(6), unique=True, nullable=False, primary_key=True)
    company = db.Column(db.String(60), nullable=False)

class Monitorlist_Index(db.Model):
    __tablename__ = 'monitorlist'
    symbol = db.Column(db.String(6), unique=True, nullable=False, primary_key=True)
    low_price = db.Column(db.Float, nullable=True)
    high_price = db.Column(db.Float, nullable=True)
    latest_reached = db.Column(db.DateTime, nullable=False)


class Quote_CSI300(db.Model):
    __tablename__ = 'quote'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    symbol = db.Column(db.String(6), db.ForeignKey("index.symbol"), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    open = db.Column(db.Float, nullable=True)
    high = db.Column(db.Float, nullable=True)
    low = db.Column(db.Float, nullable=True)
    close = db.Column(db.Float, nullable=True)
    volume = db.Column(db.BIGINT, nullable=True)


class Quote(Quote_CSI300):
    __tablename__ = 'quote'
    __table_args__ = {'extend_existing': True}

    adjusted = db.Column(db.Float, nullable=True)


class Report(db.Model):
    __tablename__ = 'report'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    symbol = db.Column(db.String(6), db.ForeignKey("index.symbol"), nullable=False)
    yr_high = db.Column(db.Boolean, nullable=True)
    yr_low = db.Column(db.Boolean, nullable=True)
    downtrend = db.Column(db.Boolean, nullable=True)
    uptrend = db.Column(db.Boolean, nullable=True)
    high_volume = db.Column(db.Boolean, nullable=True)
    rsi = db.Column(db.String(4), nullable=True)
    macd = db.Column(db.String(4), nullable=True)
    bolling = db.Column(db.String(10), nullable=True)


class Findex(db.Model):
    # Financial Index wiht Sector and Industry Information
    __tablename__ = 'findex'
    Symbol = db.Column(db.String(10), unique=True, nullable=False, primary_key=True)
    Name = db.Column(db.String(60), nullable=False)
    Sector = db.Column(db.String(60), nullable=False)
    Industry = db.Column(db.String(60), nullable=False)
    Index = db.Column(db.String(20), nullable=False)
    Secode = db.Column(db.String(10), nullable=False)
    Indcode = db.Column(db.String(10), nullable=False)


class Shares_outstanding(db.Model):
    __tablename__ = 'shares_outstanding'
    symbol = db.Column(db.String(10), nullable=False, primary_key=True, unique=True)
    shares = db.Column(db.BIGINT, nullable=True)


class Tor_report(db.Model):
    __tablename__ = 'tor_report'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    index = db.Column(db.String(20), nullable=False)
    model = db.Column(db.String(3), nullable=True)
    alert_date = db.Column(db.DateTime, nullable=True)
    recorded_start = db.Column(db.DateTime, nullable=True)
    recorded_end = db.Column(db.DateTime, nullable=True)
    recorded_tor = db.Column(db.Float, nullable=True)
    current_start = db.Column(db.DateTime, nullable=True)
    current_tor = db.Column(db.Float, nullable=True)
    volume_buffer = db.Column(db.Float, nullable=True)


class Line_report(db.Model):
    __tablename__ = 'line_report'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    index = db.Column(db.String(20), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    model = db.Column(db.String(3), nullable=True)
    type = db.Column(db.String(10), nullable=True)
    x1 = db.Column(db.DateTime, nullable=False)
    x2 = db.Column(db.DateTime, nullable=False)
    slope = db.Column(db.Float, nullable=True)
    touching = db.Column(db.Boolean, nullable=True)
    breaking = db.Column(db.Boolean, nullable=True)
    reunion = db.Column(db.Boolean, nullable=True)


class Gaps(db.Model):
    __tablename__ = 'gaps'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    index = db.Column(db.String(20), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    gap_high = db.Column(db.Float, nullable=True)
    gap_low = db.Column(db.Float, nullable=True)


class Gaps_report(db.Model):
    __tablename__ = 'gaps_report'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    index = db.Column(db.String(20), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    gap_date = db.Column(db.DateTime, nullable=False)


class Rsi_predict(db.Model):
    __tablename__ = 'rsi_predict'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    index = db.Column(db.String(20), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    current_rsi = db.Column(db.Float, nullable=True)
    target_rsi = db.Column(db.Float, nullable=True)
    target_close = db.Column(db.Float, nullable=True)
    trend = db.Column(db.String(4), nullable=True)


class Rsi_predict_report(db.Model):
    __tablename__ = 'rsi_predict_report'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    reached_date = db.Column(db.DateTime, nullable=False)
    predict_date = db.Column(db.DateTime, nullable=False)
    index = db.Column(db.String(20), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    high = db.Column(db.Float, nullable=True)
    low = db.Column(db.Float, nullable=True)
    current_rsi = db.Column(db.Float, nullable=True)
    target_rsi = db.Column(db.Float, nullable=True)
    target_close = db.Column(db.Float, nullable=True)
    trend = db.Column(db.String(4), nullable=True)


class Hvlc_report(db.Model):
    __tablename__ = 'hvlc_report'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    reached_date = db.Column(db.DateTime, nullable=False)
    index = db.Column(db.String(20), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    volchg = db.Column(db.Float, nullable=True)
    pricechg = db.Column(db.Float, nullable=True)
    vol_price_ratio = db.Column(db.Float, nullable=True)


class Ublb_cross(db.Model):
    __tablename__ = 'ublb_cross'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    date = db.Column(db.DateTime, nullable=False, primary_key=True)
    reached_date = db.Column(db.DateTime, nullable=False)
    index = db.Column(db.String(20), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)


class Hvlc_strategy(db.Model):
    __tablename__ = 'hvlc_strategy'
    date = db.Column(db.DateTime, nullable=False, primary_key=True)
    vp_ratio_low = db.Column(db.Float, nullable=True)
    vp_ratio_high = db.Column(db.Float, nullable=True)
    vr_low = db.Column(db.Float, nullable=True)
    vr_high = db.Column(db.Float, nullable=True)
    pr_low = db.Column(db.Float, nullable=True)
    pr_high = db.Column(db.Float, nullable=True)


class Hvlc_report_history(db.Model):
    __tablename__ = 'hvlc_report_history'
    id = db.Column(db.String(40), unique=True, nullable=False, primary_key=True)
    symbol = db.Column(db.String(10), nullable=False)
    index = db.Column(db.String(20), nullable=False)
    delete_date = db.Column(db.DateTime, nullable=False)
    hvlc_date = db.Column(db.DateTime, nullable=False)
    reached_date = db.Column(db.DateTime, nullable=False)
    volchg = db.Column(db.Float, nullable=True)
    pricechg = db.Column(db.Float, nullable=True)
    vol_price_ratio = db.Column(db.Float, nullable=True)
    record_rsi = db.Column(db.Float, nullable=True)
