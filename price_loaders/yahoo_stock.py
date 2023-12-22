import requests
import datetime
import pytz

import pandas as pd


def extract_price(chart: dict):
    result = chart["result"]
    time = [
        datetime.datetime.fromtimestamp(t, tz=pytz.timezone("Asia/Bangkok"))
        for t in result[0]["timestamp"]
        if t is not None
    ]
    open_price = result[0]["indicators"]["quote"][0]["open"]
    high_price = result[0]["indicators"]["quote"][0]["high"]
    low_price = result[0]["indicators"]["quote"][0]["low"]
    close_price = result[0]["indicators"]["quote"][0]["close"]
    volume = result[0]["indicators"]["quote"][0]["volume"]
    return {
        "time": time,
        "Open": open_price,
        "High": high_price,
        "Low": low_price,
        "Close": close_price,
        "Volume": volume,
    }


def requests_stock_data(
    symbol: str,
    period_start: datetime = datetime.datetime.fromtimestamp(0),
    period_end: datetime = datetime.datetime.now(),
    proxies={},
    **option,
) -> requests.Response:
    """
    Get stock data for Yahoo finace
    symbol (str: required): symbol of Thai stock, add '.BK' in case of SET
    start, end (datetime: optional): start and end date.
        By default, they are 0 and now respectively.
    :return
    response
    """
    headers = {
        "authority": "query1.finance.yahoo.com",
        "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',  # noqa
        "sec-ch-ua-mobile": "?0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",  # noqa
        "accept": "*/*",
        "origin": "https://finance.yahoo.com",
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https://finance.yahoo.com/quote/DELTA.BK?p=DELTA.BK&.tsrc=fin-srch",  # noqa
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    }
    res = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        headers=headers,
        params={
            "symbol": symbol,
            "period1": int(period_start.timestamp()),
            "period2": int(period_end.timestamp()),
            #             "range": "ytd",
            "interval": "1d",
            "includePrePost": "true",
            "events": "div%7Csplit%7Cearn",
            "lang": "en-US",
            "region": "TH",
            "crumb": "ikHMww39GL1",
            "corsDomain": "finance.yahoo.com",
        },
        proxies=proxies,
    )
    res.raise_for_status()
    result = res.json()
    chart = result["chart"]
    price_data = extract_price(chart)
    price_data = pd.DataFrame(price_data)
    price_data = price_data.set_index("time").resample("1D").mean().dropna()

    price_data.name = symbol

    return price_data


def get_stock_data(
    symbols: list, from_date: datetime.datetime, to_date: datetime.datetime
):
    return [
        requests_stock_data(symbol, from_date, to_date) for symbol in symbols
    ]
