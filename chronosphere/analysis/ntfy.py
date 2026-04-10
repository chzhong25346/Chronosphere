import datetime as dt
import logging
import re
import requests
import pandas as pd
from dateutil import parser

logger = logging.getLogger('main.ntfy')


def sendNtfy_Message(sub, message, topic):
    day = dt.datetime.today().strftime("%Y-%m-%d")
    dow = parser.parse(day).strftime("%a")
    today = f"{day} {dow}"

    title = f"{today} {sub}"

    lines = []
    for line in message:
        match = re.search(r'([↑↓])(\d+)', line)
        if match:
            arrow = match.group(1)
            value = match.group(2)
            lines.append(f"{line}")
        else:
            lines.append(line)

    payload = (
        "\n".join(lines)
        + "\n\n⚠️ 注意事项:\n"
          "1. 关注信号后有没有大量（超过这一天）\n"
          "2. 长周期打水漂\n"
          "3. 短周期变盘快"
    )

    url = f"https://ntfy.sh/{topic.lstrip('/')}"

    try:
        requests.post(
            url,
            headers={
                "Title": title
            },
            data=payload.encode("utf-8"),
            timeout=5
        )
    except Exception as e:
        logger.error(f"ntfy send failure: {e}")
        raise


def sendNtfy(pick_dic, topic):
    day = dt.datetime.today().strftime("%Y-%m-%d")
    dow = parser.parse(day).strftime("%a")
    title = f"{day} {dow}"

    text = generate_text(pick_dic)
    url = f"https://ntfy.sh/{topic.lstrip('/')}"

    try:
        requests.post(
            url,
            headers={
                "Title": title,
            },
            data=text.encode("utf-8"),
            timeout=5
        )
        logger.info("Sent ntfy notification")
    except Exception as e:
        logger.error(f"ntfy send failure: {e}")


def generate_text(d):
    df = pd.DataFrame.from_dict(d, orient="index").replace({None: "-"})
    table = df.to_string()

    return f"""
Weekly Picks / Monitor Alert

{table}

筛选条件:
- EPS > 0
- EPS <= 15
- P/B <= 1.59
- P/S <= 2
- D/E ratio <= 50
- Dividend Yield >= 4.05
- Payout Ratio >= 25
- Beta <= 1.2
""".strip()
