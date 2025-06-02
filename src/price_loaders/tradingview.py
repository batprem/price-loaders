import websocket
from websocket import create_connection

import json
import random
import string
import re


import logging
from typing import List, Iterator, Literal
import requests

import pandas as pd
import numpy as np
from typing import Optional
import datetime
import pytz


AvailableTimeFrame = Literal[
    "1",
    "3",
    "5",
    "15",
    "45",
    "1h",
    "2h",
    "3h",
    "4h",
    "1D",
    "1W",
    "1M",
    "3M",
    "6M",
    "12M",
]


class SESSION_ENUM:
    """
    Description:
        Session constants
    """

    WEBSOCKET: bool = False
    CHART: bool = True


headers = json.dumps(
    {
        "Origin": "https://data.tradingview.com",
    }
)


def generate_sesssion(session) -> str:
    """
    Description:
        Get {session prefix}_{random string}
    Args:
        session (bool)
            True: for websocket
            False: for chart
    Returns:
        (str) web socket session
    """
    string_length = 12
    letters = string.ascii_lowercase
    random_string = "".join(
        random.choice(letters) for i in range(string_length)
    )
    prefix = "cs" if session else "qs"
    return f"{prefix}_{random_string}"


def prepend_header(sentences: str) -> str:
    """
    Description:
        format data into websocket message:
    Args:
        sentence
            (str) contructed message
    Returns:
        (str) An added prefix message
    Example:
        ~m~54~m~{"m":"set_auth_token","p":["unauthorized_user_token"]}
    """
    return f"~m~{len(sentences)}~m~{sentences}"


def construct_message(function_name: str, parameters: List[str | int | dict]) -> str:
    """
    Args:
        function_name
            (str) Function to summit into websocket
        parameters:
            List[str]: list paramaters to input into the function
    Returns:
        (str) a message as a JSON format join without space
    Example:
        {"m":"set_auth_token","p":["unauthorized_user_token"]}
    """
    transformed_params = []
    for parameter in parameters:
        if isinstance(parameter, str):
            transformed_params.append(parameter)
        elif isinstance(parameter, dict):
            transformed_params.append(json.dumps(parameter).replace("/", ""))
        else:
            transformed_params.append(parameter)  # type: ignore
    return json.dumps(
        {"m": function_name, "p": transformed_params}, separators=(",", ":")
    )  # .replace('"(', '{').replace('}"', '}')


def create_message(function_name: str, parameters: List[str | int | dict]) -> str:
    """
    Description:
        Integration of a created message function
    Args:
        function_name:
            (str) Function to summit into websocket
        parameters:
            List[str]: list paramaters to input into the function
    Returns:
        (str) message as websocket message format
    Example:
        ~m~54~m~{"m":"set_auth_token","p":["unauthorized_user_token"]}
    """
    output = prepend_header(construct_message(function_name, parameters))
    return output


def send_message(
    ws: websocket._core.WebSocket, func: str, args: List[str | int | dict]
) -> None:
    """
    Description:
        Send formatted message
    Params:
        ws:
            (websocket._core.WebSocket) web socket sesssoin
        func:
            (str) Function to summit into websocket
        args:
            List[str]: list paramaters to input into the function
    """

    ws.send(create_message(func, args))


def get_pe_ratio_template():
    headers = {
        "authority": "pine-facade.tradingview.com",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-GB,en;q=0.9",
        # 'if-none-match': 'W/"rscUUmqLJOxhFguZvk588Rqfz37Es9R6f9UjLoBPJrgOHvnMmnx3/MwXZrOCaitLpw8ogsgzOhaGQ4ALxt7xnA=="', # noqa
        "origin": "https://www.tradingview.com",
        "referer": "https://www.tradingview.com/",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',  # noqa
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36",  # noqa
    }

    response = requests.get(
        "https://pine-facade.tradingview.com/pine-facade/translate/STD%3BFund_price_earnings/38.0/",  # noqa
        headers=headers,
    )
    return response.json()["result"]["ilTemplate"]


def get_pe_ratio_message(
    chart_session, chart_session_name, study_name="pe_ratio"
):
    text = get_pe_ratio_template()
    parameters = json.dumps(
        {
            "text": text,
            "pineId": "STD;Fund_price_earnings",
            "pineVersion": "38.0",
            "in_0": {"v": "", "f": True, "t": "symbol"},
            "__user_pro_plan": {"v": "", "f": True, "t": "usertype"},
        }
    )

    message = (
        '{"m":"create_study","p":["'
        + chart_session
        + f'","{study_name}","st1","{chart_session_name}","Internal@tv-scripting-101!",'  # noqa
        + parameters
        + "]}"
    )

    message = f"~m~{len(message)}~m~{message}"
    return message


def request_data(
    symbol: str,
    time_frame: str,
    look_back_bars: int,
    websocket_session,
    chart_session,
):
    # look_back_bars = 365 * 10

    resolve_symbol = json.dumps({"symbol": symbol, "adjustment": "splits"})

    # Set up session
    chart_session_name = "price"
    ws = create_connection(
        "wss://data.tradingview.com/socket.io/websocket", headers=headers
    )

    send_message(ws, "set_auth_token", ["unauthorized_user_token"])
    send_message(ws, "chart_create_session", [chart_session, ""])
    send_message(ws, "quote_create_session", [websocket_session])

    send_message(
        ws,
        "quote_add_symbols",
        [websocket_session, symbol, json.dumps({"flags": ["force_permission"]})],
    )

    send_message(
        ws, "resolve_symbol", [chart_session, "symbol_1", f"={resolve_symbol}"]
    )
    logging.debug(f"Time frame {time_frame}")
    send_message(
        ws,
        "create_series",
        [
            chart_session,
            chart_session_name,
            chart_session_name,
            "symbol_1",
            time_frame,
            look_back_bars,
        ],
    )
    return ws


def listen(
    symbol: str, look_back_bars: int, time_frame: AvailableTimeFrame
) -> Iterator[dict]:
    """
    Open a websocket channel and wait for return.


    Args:
        symbol (str): Target symbol
        look_back_bars (int): Number of bar to look back
        time_frame (str)

    Yields:
        Iterator[dict]: Yield dictionary until it found stop
            the stop condition.
    """
    websocket_session = generate_sesssion(SESSION_ENUM.WEBSOCKET)
    logging.debug(f"Web socket session generated: {websocket_session}")

    chart_session = generate_sesssion(SESSION_ENUM.CHART)
    chart_session_name = "price"
    # chart_session = "cs_j11Xz8aCaxfT"
    logging.debug(f"Chart session generated: {chart_session}")

    ws = request_data(
        symbol, time_frame, look_back_bars, websocket_session, chart_session
    )
    ws.send(get_pe_ratio_message(chart_session, chart_session_name))
    while True:
        try:
            results: str = ws.recv()
            logging.debug(results)
            pattern = re.compile("~m~\d+~m~~h~\d+$")  # noqa

            if pattern.match(results):
                # Send heart beat to keep connection alive
                ws.recv()
                ws.send(results)

            for r in results.split("~m~"):
                try:
                    r = json.loads(r)
                    if not isinstance(r, dict):
                        continue
                    message = r.get("m")
                    if (
                        message == "timescale_update"
                        or message == "du"
                        or message == "study_error"
                    ):
                        yield r
                        # return r
                except json.JSONDecodeError:
                    pass
        except KeyboardInterrupt:
            logging.debug("End")
            break


def load_raw_data(
    symbol: str, look_back_bars: int, time_frame: AvailableTimeFrame
) -> dict:
    """Load price as raw data.

    Args:
        symbol (str): Target symbol
        look_back_bars (int): Number of look bars
        time_frame (AvailableTimeFrame): Observing timeframe

    Returns:
        dict: Raw chart data
    """
    chart = {}

    for res in listen(symbol, look_back_bars, time_frame):
        m = res["m"]
        if m == "study_error":
            break
        p = res["p"]
        data = [element for element in p if isinstance(element, dict)][0]
        if not (("price" in chart) and ("price" in data)):
            chart = {**chart, **data}
        if "pe_ratio" in chart:
            break
    return chart  # type: ignore


def extract_price(chart: dict) -> pd.DataFrame:
    """Extract ohlcv chart.

    Args:
        chart (dict): Raw chart data.

    Returns:
        pd.DataFrame: Result dataframe
    """
    return pd.DataFrame(
        [st["v"] for st in chart["price"]["s"]],
    ).rename(
        columns={
            0: "time",
            1: "open",
            2: "high",
            3: "low",
            4: "close",
            5: "volume",
        }
    )


def extract_pe_ratio(chart: dict) -> Optional[pd.DataFrame]:
    """Extracting PE ratio from RAW chart

    Args:
        chart (dict): raw chart data

    Returns:
        Optional[pd.DataFrame]:
         - None if pe_ratio not found in the chart data
         - Dataframe if from
    """
    if "pe_ratio" not in chart:
        return None
    df = pd.DataFrame(
        [st["v"] for st in chart["pe_ratio"]["st"]],
        columns=["time", "pe_ratio"],  # type: ignore
    )
    df["pe_ratio"] = df["pe_ratio"].astype(np.float64)
    return df


def aggregate_to_dataframe(
    chart: dict, timezone: Optional[pytz.BaseTzInfo] = None
) -> pd.DataFrame:
    """Aggregate raw chart data in a Pandas dataframe.

    Args:
        chart (dict): Raw chart data
        timezone (Optional[pytz.timezone], optional):
            Operating timezone. Defaults to None.

    Returns:
        pd.DataFrame: Return as a dataframe
    """
    if timezone is None:
        timezone = pytz.timezone("Asia/Bangkok")
    ohlcv = extract_price(chart)
    logging.debug(ohlcv)
    pe_ratio = extract_pe_ratio(chart)
    if pe_ratio is not None:
        ohlcv = pd.concat(
            [ohlcv.set_index("time"), pe_ratio.set_index("time")], axis=1
        ).reset_index()

    ohlcv.time = ohlcv.time.apply(  # type: ignore
        lambda timestamp: datetime.datetime.fromtimestamp(
            timestamp, tz=timezone
        )
    )
    ohlcv = ohlcv.dropna(subset=["open", "high", "low", "close"])
    ohlcv[["open", "high", "low", "close"]] = ohlcv[
        ["open", "high", "low", "close"]
    ].astype(np.float64)
    return ohlcv


def load_asset_price(
    symbol: str,
    look_back_bars: int,
    time_frame: AvailableTimeFrame,
    timezone: Optional[pytz.BaseTzInfo] = None,
) -> pd.DataFrame:
    """Run full process to scrape TradingView data

    Args:
        symbol (str): Asset symbol
        look_back_bars (int): Number of look back bars
        time_frame (AvailableTimeFrame): Time frame
        timezone (Optional[pytz.timezone], optional): Timezone.
            Defaults to None.

    Returns:
        pd.DataFrame: return as a dataframe
    """
    chart = load_raw_data(symbol, look_back_bars, time_frame)
    return aggregate_to_dataframe(chart, timezone)


def get_kbank():
    df = load_asset_price("SET:KBANK", 365, "1D")
    print(df)


if __name__ == "__main__":
    get_kbank