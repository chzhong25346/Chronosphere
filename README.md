# Chronosphere

You are aware, ja, of my Chronosphere, a device which is capable of moving matter through time and through space!
- Professor Einstein

## Getting Started

Python3.5 or higher version and Pandas, Numpy, SQLAlchemy and ect.

```
DB_USER="DB_USER"
DB_PASS="DB_PASSWORD"
DB_HOST="localhost"
DB_PORT="3306"
EMAIL_USER="SENDER_GMAIL"
EMAIL_PASS="SENDER_GMAIL_PWD"
EMAIL_TO="MYDOG@GMAIL.COM,MYCAT@GMAIL.COM"
AV_KEY="ALPHAVANTAGE_KEY"
```

### Prerequisites

What things you need to install the software and how to install them

```
python3.5+
Please see requirements and "pip install -r requirements.txt"
```


### Usage

1.  -t/--turnover market(china/canada/usa) : Turnover Ratio Analysis
2.  -l/--line market(china/canada/usa) : Support and Resistance Line Analysis
3.  -g/--gap market(china/canada/usa) : Gaps created with range
4.  -r/--rsi market(china/canada/usa/eei) : RSI prediction
5.  -v/--hvlc market(china/canada/usa/eei) : High Volume Low Change
6.  -u/--ublb market(china/canada/usa/eei) : Up Band Lower Band Cross


### Tables

Index -- Quote --> RSI Predict --> RSI Predict Report(reached) --> UBLB Cross --> HVLC Report

## Authors

* **Colin Zhong** - *Initial work* - [Git Page](https://github.com/chzhong25346)
