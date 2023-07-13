import pandas as pd
import requests
import time
import os
import io


def get_coin_names():
    return ["BTC", "ETH", "ETC", "LTC", "DOGE"]


def timescale(period="daily", interval=None, asset_type="coins"):
    """ Compute timescale to scale estimates by depending on period/interval of time-series data.
    """

    intraday_length = 7.5
    num_weekdays = 5
    year_length = 252
    if asset_type == "coins":
        intraday_length = 24
        num_weekdays = 7
        year_length = 365
    elif asset_type == "stocks":
        intraday_length = 7.5
        num_weekdays = 5
        year_length = 252
    # For intraday, w=e convert to daily figures
    if period == "intraday":
        if interval == "1min":
            time_scale = 1 / (intraday_length * 60)
        elif interval == "5min":
            time_scale = 1 / (intraday_length * 12)
        elif interval == "15min":
            time_scale = 1 / (intraday_length * 4)
        elif interval == "30min":
            time_scale = 1 / (intraday_length * 2)
        elif interval == "60min":
            time_scale = 1 / intraday_length
        else:
            raise ValueError("'interval' must be one of 1min, 5min, 15min, 30min, or 60min")
    elif period == "daily":  # For daily/weekly, we convert to annual figures
        time_scale = 1 / year_length  # Crypto trades year-round.
    elif period == "weekly":
        time_scale = 1 / (year_length / num_weekdays)
    else:
        raise ValueError("'period' must be intraday, daily, or weekly")
    return time_scale


def get_yahoo_quote(symbol):
    """
    AV's GLOBAL_QUOTE function is lagged one day, so it is practically useless. Thus, we provide a wrapper
    to Yahoo finance's API solely for the get quote function. This returns the midpoint of the bid and ask.

    Parameters
    :param symbol: the stock ticker symbol
    :return: float, the current market price via Yahoo Quotes
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    headers = {'User-agent': 'Mozilla/5.0'}
    payload = {"symbols": symbol}
    j = requests.get(url, payload, headers=headers)
    data_list = j.json()['quoteResponse']['result'][0]
    bid = float(data_list["bid"])
    ask = float(data_list["ask"])
    return (bid + ask) / 2


def get_yahoo_quote_v10(ticker):
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=price"
    headers = {'User-agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    content_type = response.headers['Content-Type']
    if content_type == 'application/json;charset=utf-8' or 'application/json':
        data = response.json()
        try:
            price = data["quoteSummary"]["result"][0]["price"]["regularMarketPrice"]["raw"]
            return price
        except (KeyError, TypeError):
            raise Exception("Error: Unable to fetch stock price data.")
    else:
        raise Exception("Error: Unable to fetch stock price data.")


class av:
    def __init__(self, tier="premium"):
        """
        Create an Alpha Vantage object that can download stock and cryptocurrency prices using
        Alpha Vantage's free or premium API. The user must specify 'tier' as either 'free' or 'premium'.

        :param tier: The user must specify 'tier' as either 'free' or 'premium'.
        """
        self.tier = tier
        self.key_type = str.upper(self.tier) + "_API_KEY"
        self.api = "https://www.alphavantage.co/query"

    def log_in(self, key):
        """
        Set your free or premium API key for AV as an environment variable for the current session.

        (Parameters)
        'key' must be the API key given when you sign up for access to Alpha Vantage's API.
        :param key:
        :return: None
        """
        if self.tier != "free" and self.tier != "premium":
            raise ValueError("'type' must be set to 'free' or 'premium'")
        os.environ[self.key_type] = key
        print("Welcome to our Alpha Vantage python interface.")

    def get_stock_quote(self, symbol, datatype="csv"):
        """
        Look up a stock quote for a given symbol.

        Parameters
        :param symbol: the stock symbol to look up, a string, e.g. "SPY", etc.
        :param datatype: either 'csv' or 'json'.
        :return: pandas DataFrame of the stock quote
        """
        api_key = os.getenv(self.key_type)
        # Define the payload for the GET query
        payload = {"function": "GLOBAL_QUOTE",
                   "symbol": symbol,
                   "apikey": api_key,
                   "datatype": datatype
                   }
        js = requests.get(self.api, payload)
        if datatype == "csv":
            s = js.content
            w = pd.read_csv(io.StringIO(s.decode("utf-8")))
        elif datatype == "json":
            s = js.content
            w = pd.read_json(s)
        else:
            raise ValueError("'datatype' must be csv or json")
        return w

    def _get_stock(self, symbol, period="daily", interval=None, adjusted=True, datatype="csv"):
        """
        Pass a stock symbol (string), period (string), datatype to retrieve a
        time-series of close prices from AV. The argument 'period' must be either
        'intraday', 'daily', 'weekly', 'monthly'. If intraday is used you must past an "interval".


        :param symbol: a stock symbol like SPY or DIS
        :param period: "intraday", "daily", "weekly", "monthly"
        :param interval: time interval: "1min", "5min", etc
        :param adjusted: boolean for raw prices or adjusted prices
        :param datatype: "csv" or "json"
        :return: DataFrame
        """

        if period == "intraday":
            if interval is None or (interval not in ["1min", "5min", "15min", "30min", "60min"]):
                raise ValueError("interval must be 1min, 5min, 15min, 30min, 60min for intraday data")

        per = period
        api_key = os.getenv(self.key_type)
        # All periods besides intraday have adjusted prices available.
        if (period == "daily" or period == "weekly" or period == "monthly") and adjusted:
            per = period + "_adjusted"
        payload = {"function": "TIME_SERIES_" + str.upper(per),
                   "symbol": symbol,
                   "outputsize": "full",
                   "datatype": datatype,
                   "apikey": api_key
                   }
        # The payloads are identical for all periods except that, an adjusted boolean is added
        if period != "intraday":
            payload["adjusted"] = adjusted
        else:
            payload["interval"] = interval
        js = requests.get(self.api, payload)
        if datatype == "csv":
            s = js.content
            w = pd.read_csv(io.StringIO(s.decode("utf-8")))
            w = w[::-1]
            w["timestamp"] = pd.to_datetime(w["timestamp"])
            w.set_index("timestamp", inplace=True)
            w.index.to_pydatetime()
            return w
        elif datatype == "json":
            s = js.content
            w = pd.read_json(s)
            return w

    def _get_stocks(self, symbols, period="daily", interval=None, adjusted=True, what="close"):
        """
        Download a collection of stocks
        :param symbols: list of stock tickers
        :param period: period of prices
        :param interval: time interval for intraday period
        :param adjusted: boolean for adjusted prices
        :param what: which price to pull, close, open, etc
        :return: pandas DataFrame of stock prices
        """
        stocks = []
        if len(symbols) <= 30:
            stocks = [self._getStock(i, period=period, interval=interval, adjusted=adjusted)[what] for i in symbols]
        else:
            for i in range(len(symbols)):
                stocks.append(self._getStock(symbols[i], period=period, interval=interval, adjusted=adjusted)[what])
                if i % 30 == 0:
                    time.sleep(60)
        w = pd.DataFrame(data=stocks).transpose()
        w.columns = symbols
        w.dropna(axis=0, how='any', inplace=True)
        return w

    def _get_coin(self, symbol, period="daily", interval=None, market="USD", datatype="csv"):
        """
        Download a time-series of cryptocurrencies. Available periods are "daily", "weekly",
        "monthly", "yearly" and "intraday" which returns one-minute intervals.
        
        All prices are refreshed daily at midnight UTC except intraday which returns the most recent minute price.
        """
        if period == "intraday":
            if interval is None or (interval not in ["1min", "5min", "15min", "30min", "60min"]):
                raise ValueError("interval must be 1min, 5min, 15min, 30min, 60min for intraday data")
        api_key = os.getenv(self.key_type)
        per = str.upper(period)

        if period != "intraday":
            function_name = "DIGITAL_CURRENCY_" + per
        else:
            function_name = "CRYPTO_" + per

        payload = {"function": function_name,
                   "symbol": symbol,
                   "market": market,
                   "outputsize": "full",
                   "datatype": datatype,
                   "apikey": api_key
                   }
        if period == "intraday":
            payload["interval"] = interval

        js = requests.get(self.api, payload)
        if datatype == "csv":
            s = js.content
            w = pd.read_csv(io.StringIO(s.decode("utf-8")))
            w = w[::-1]
            w["timestamp"] = pd.to_datetime(w["timestamp"], utc=True)
            w.set_index("timestamp", inplace=True)
            w.index.to_pydatetime()
            # All but intraday have extra columns we do not need.
            if period != "intraday":
                w.columns = ["open", "high", "low", "close", "open.1", "high.1", "low.1", "close.1", "volume",
                             "market_cap"]
                w.drop(labels=["open.1", "high.1", "low.1", "close.1"], axis=1, inplace=True)
            return w
        elif datatype == "json":
            s = js.content
            w = pd.read_json(s)
            return w

    def _get_coins(self, symbols, period="daily", interval=None, what="close"):
        """ Download a collection of cryptocurrencies from Alpha-Vantage."""
        if what == "adjusted_close":
            raise ValueError(
                "Cryptocurrencies do not have adjusted close prices. "
                "Pass what='close' instead, or anything from OHLCV.")

        if len(symbols) <= 30:
            coins = [self._getCoin(i, period=period, interval=interval)[what] for i in symbols]
        else:
            raise ValueError("TODO: implement sleep for AV api between batches of 30 stocks")
        w = pd.DataFrame(data=coins).transpose()
        w.columns = symbols
        w.dropna(axis=0, how='any', inplace=True)
        return w

    def get_assets(self, symbols, period="daily", interval=None, adjusted=True, what="close", datatype="csv"):
        """
        Download financial time-series data from Alpha-Vantage for stocks and cryptocurrencies. This function
        can either take single-assets to download all OHLC data for or multiple assets returning just one
        column of OHLC.
        
        All cryptocurency prices are refreshed daily at midnight UTC except intraday which returns the most
        recent minute price. Stocks are lagged one day.

        (Parameters)
        'symbols' string of asset ticker or list of asset tickers
        'period' either 'intraday', 'daily', 'weekly', 'monthly'
        'interval' either '1min', '5min', '15min', '30min', or '60min'
        'adjusted' boolean for adjusted prices (only for non-intraday stock time-series)
        'what' OHLC column name: 'open', 'high', 'low', 'close', 'volume' and 'adjusted_close'
        'datatype' 'csv' or 'json'. Default is 'csv'.
        """

        # If we pass a string, this code will fail for one-asset, so let's wrap single-string input into a list
        if type(symbols) == str:
            symbols = [symbols]
        n = len(symbols)

        if n == 1:
            if symbols[0] in self.getCoinNames():
                return self._get_coin(symbols[0], period, interval, market="USD", datatype=datatype)
            else:
                return self._get_stock(symbols[0], period, interval, adjusted, datatype)
        elif n > 1:
            if symbols[0] in get_coin_names():
                return self._get_coins(symbols, period, interval, what=what)
            else:
                return self._get_stocks(symbols, period, interval, adjusted, what)
        else:
            raise ValueError("Empty symbols input.")
