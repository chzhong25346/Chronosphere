import datetime as dt
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from dateutil import parser

logger = logging.getLogger('main.email')

def sendMail_Message(object, sub, message):
    # today's datetime
    day = dt.datetime.today().strftime("%Y-%m-%d")
    dow = parser.parse(day).strftime("%a")
    today = day + ' ' + dow
    # start talking to the SMTP server for Gmail
    context = ssl.create_default_context()

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls(context=context)
    s.ehlo()
    # now login as my gmail user
    user = object.EMAIL_USER
    pwd = object.EMAIL_PASS
    # rcpt = object.EMAIL_TO
    rcpt = [i for i in object.EMAIL_TO.split(',')]
    try:
        s.login(user,pwd)
    except Exception as e:
        logger.error(e)

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = today + " " + sub
    msg['From'] = user
    msg['To'] = ", ".join(rcpt)

    # if picks is a list like ['PSK.TO', 'ABC.TO']
    body_plain = "\n".join(message)
    attachment = MIMEText(body_plain, 'plain', 'utf-8')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(attachment)

    # send the email
    s.sendmail(user, rcpt, msg.as_string())
    # we're done
    s.quit()
    logger.info("Sent email")


def sendMail(object, pick_dic):
    # today's datetime
    day = dt.datetime.today().strftime("%Y-%m-%d")
    dow = parser.parse(day).strftime("%a")
    today = day + ' ' + dow
    # start talking to the SMTP server for Gmail
    context = ssl.create_default_context()

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls(context=context)
    s.ehlo()
    # now login as my gmail user
    user = object.EMAIL_USER
    pwd = object.EMAIL_PASS
    # rcpt = object.EMAIL_TO
    rcpt = [i for i in object.EMAIL_TO.split(',')]
    try:
        s.login(user,pwd)
    except Exception as e:
        logger.error(e)

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = today
    msg['From'] = user
    msg['To'] = ", ".join(rcpt)

    html = generate_html(pick_dic)
    attachment = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(attachment)

    # send the email
    s.sendmail(user, rcpt, msg.as_string())
    # we're done
    s.quit()
    logger.info("Screener sent email")


def generate_html(d):
    df = pd.DataFrame.from_dict(d, orient='index').replace({None:'-'})
    html = """\
    <html>
    <head></head>
    <body>
        <h2>Weekly Picks / Monitor Alert</h2>

        {picked}<br>

        <h6> EPS > 0 <br>
          EPS > 0 <br>
          EPS <= 15 <br>
          P/B <= 1.59 <br>
          P/S <= 2 <br>
          D/E ratio <= 50 <br>
          Dividend Yield >= 4.05 <br>
          Payout Ratio >= 25<br>
          Beta <= 1.2 <br>
        </h6>

    </body>
    </html>
    """
    html = html.format(picked=df.to_html())
    return html
