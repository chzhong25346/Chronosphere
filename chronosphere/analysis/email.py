import smtplib, ssl
import logging
import yaml,os
import sys
import datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dateutil import parser
import pandas as pd
logger = logging.getLogger('main.email')

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
        <h2>Weekly Picks</h2>

        {picked}<br>

        <h6 > EPS > 0 <br>
          PE <= 15 <br>
          PB <= 1.5 <br>
          PE*PB <= 22.5 <br>
          DE ratio <= 50 <br>
          Dividend Yield >= 4 <br>
          Payou tRatio >= 25<br>
          Beta 5y Monthly <= 1 <br>
          market Cap >= 3B</h6>

    </body>
    </html>
    """
    html = html.format(picked=df.to_html())
    return html
