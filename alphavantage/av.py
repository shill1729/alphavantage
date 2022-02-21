import requests
import os
import io
import pandas as pd
import time

class av:
    def __init__(self, tier = "premium"):
        """ Create an Alpha Vantage object that can download stock and cryptocurrency prices using
        Alpha Vantage's free or premium API. The user must specify 'tier' as either 'free' or 'premium'.
        """
        self.tier = tier
        self.keytype = str.upper(self.tier)+"_API_KEY"
        self.api = "https://www.alphavantage.co/query"

    def log_in(self, key):
        """ Set your free or premium API key for AV as an environment variable for the current session.

        (Parameters)
        'key' must be the API key given when you sign up for access to Alpha Vantage's API.
        """
        if self.tier != "free" and self.tier != "premium":
            raise ValueError("'type' must be set to 'free' or 'premium'")
        os.environ[self.keytype] = key
        print("Welcome to our Alpha Vantage python interface.")

    def getStockQuote(self, symbol, datatype = "csv"):
        """ Look up a stock quote for a given symbol.

        (Parameters)
        'symbol' the stock symbol to look up, a string, e.g. "SPY", etc.
        'datatype' either 'csv' or 'json'.
        """
        apiKey = os.getenv(self.keytype)
        # Define the payload for the GET query
        payload = {"function":"GLOBAL_QUOTE", 
                    "symbol":symbol, 
                    "apikey":apiKey, 
                    "datatype":datatype
                    }
        js = requests.get(self.api, payload)
        if datatype == "csv":
            s = js.content
            w = pd.read_csv(io.StringIO(s.decode("utf-8")))
        elif datatype == "json":
            s = js.content
            w = pd.read_json(s)
        return w

    def getYahooQuote(self, symbol):
        """ AV's GLOBAL_QUOTE function is lagged one day, so it is practically useless. Thus, we provide a wrapper
        to Yahoo finance's API solely for the get quote function. This returns the midpoint of the bid and ask.
        """
        url = "https://query1.finance.yahoo.com/v7/finance/quote"
        headers = {'User-agent': 'Mozilla/5.0'}
        payload = {"symbols":symbol}
        j = requests.get(url, payload, headers = headers)
        data_list = j.json()['quoteResponse']['result'][0]
        bid = float(data_list["bid"])
        ask = float(data_list["ask"])
        return (bid+ask)/2


    def _getStock(self, symbol, period = "daily", interval  = None, adjusted = True, datatype = "csv"):
        """ Pass a stock symbol (string), period (string), datatype to retrieve a 
        time-series of close prices from AV. The argument 'period' must be either 
        'intraday', 'daily', 'weekly', 'monthly'. If intraday is used you must past an "interval".

        
        """

        if period == "intraday":
            if interval is None or (interval not in ["1min", "5min", "15min", "30min", "60min"]):
                raise ValueError("interval must be 1min, 5min, 15min, 30min, 60min for intraday data")

        per = period
        priceKey = ""
        apiKey = os.getenv(self.keytype)
        # All periods besides intraday have adjusted prices available.
        if (period == "daily" or period == "weekly" or period == "monthly") and adjusted:
            per = period+"_adjusted"
        payload = {"function":"TIME_SERIES_"+str.upper(per),
                    "symbol": symbol,
                    "outputsize": "full",
                    "datatype":datatype,
                    "apikey":apiKey 
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
            w.set_index("timestamp", inplace = True)
            w.index.to_pydatetime()
            return w
        elif datatype == "json":
            s = js.content
            w = pd.read_json(s)
            return w


    def _getStocks(self, symbols, period = "daily", interval = None, adjusted = True, what = "close"):
        """ Download a collection of stocks"""
        stocks = []
        if len(symbols) <= 30:
            stocks = [self._getStock(i, period = period, interval = interval, adjusted = adjusted)[what] for i in symbols]
        else:
            for i in range(len(symbols)):
                stocks.append(self._getStock(symbols[i], period = period, interval = interval, adjusted = adjusted)[what])
                if i%30==0:
                    time.sleep(60)
            #raise ValueError("TODO: implement sleep for AV api between batches of 30 stocks")
            
        w = pd.DataFrame(data = stocks).transpose()
        w.columns = symbols
        w.dropna(axis = 0, how = 'any', inplace = True)
        return w

    def _getCoin(self, symbol, period = "daily", interval = None, market = "USD", datatype = "csv"):
        """" Download a time-series of cryptocurrencies. Available periods are "daily", "weekly",
        "monthly", "yearly" and "intraday" which returns one-minute intervals.
        
        All prices are refrehsed daily at midnight UTC except intraday which returns the most recent minute price.
        """
        if period == "intraday":
            if interval is None or (interval not in ["1min", "5min", "15min", "30min", "60min"]):
                raise ValueError("interval must be 1min, 5min, 15min, 30min, 60min for intraday data")
        apiKey = os.getenv(self.keytype)
        per = str.upper(period)
        function_name = ""
        if period != "intraday":
            function_name = "DIGITAL_CURRENCY_"+per
        else:
            function_name = "CRYPTO_"+per

        payload = {"function":function_name,
                    "symbol": symbol,
                    "market": market,
                    "outputsize": "full",
                    "datatype":datatype,
                    "apikey": apiKey 
                    }
        if period == "intraday":
            payload["interval"] = interval
        
        js = requests.get(self.api, payload)
        if datatype == "csv":
            s = js.content
            w = pd.read_csv(io.StringIO(s.decode("utf-8")))
            w = w[::-1]
            w["timestamp"] = pd.to_datetime(w["timestamp"], utc = True)
            w.set_index("timestamp", inplace = True)
            w.index.to_pydatetime()
            # All but intraday have extra columns we do not need.
            if period != "intraday":
                w.columns = ["open", "high", "low", "close", "open.1", "high.1", "low.1", "close.1", "volume", "market_cap"]
                w.drop(labels = ["open.1", "high.1", "low.1", "close.1"], axis = 1, inplace = True)
            return w
        elif datatype == "json":
            s = js.content
            w = pd.read_json(s)
            return w

    def _getCoins(self, symbols, period = "daily", interval = None, what = "close"):
        if what == "adjusted_close":
            raise ValueError("Cryptocurrencies do not have adjusted close prices. Pass what='close' instead, or anything from OHLCV.")
        """ Download a collection of cryptocurrencies from Alpha-Vantage."""
        if len(symbols) <= 30:
            coins = [self._getCoin(i, period = period, interval = interval)[what] for i in symbols]
        else:
            raise ValueError("TODO: implement sleep for AV api between batches of 30 stocks")
        w = pd.DataFrame(data = coins).transpose()
        w.columns = symbols
        w.dropna(axis = 0, how = 'any', inplace = True)
        return w

    def getCoinNames(self):
        return ["BTC", "ETH", "ETC", "LTC",  "DOGE"]

    def timescale(self, period = "daily", interval = None, asset_type = "coins"):
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
                time_scale = 1/(intraday_length*60)
            elif interval == "5min":
                time_scale = 1/(intraday_length*12)
            elif interval == "15min":
                time_scale = 1/(intraday_length*4)
            elif interval == "30min":
                time_scale = 1/(intraday_length*2)
            elif interval == "60min":
                time_scale = 1/intraday_length
        elif period == "daily": # For daily/weekly, we convert to annual figures
            time_scale = 1/year_length # Crypto trades year-round.
        elif period == "weekly":
            time_scale = 1/(year_length/num_weekdays)
        return time_scale

    def getAssets(self, symbols, period = "daily", interval = None, adjusted = True, what = "close", datatype = "csv"):
        """" 
        Download financial time-series data from Alpha-Vantage for stocks and cryptocurrencies. This function can either
        take single-assets to download all OHLC data for or multiple assets returning just one column of OHLC.
        
        All cryptocurency prices are refrehsed daily at midnight UTC except intraday which returns the most recent minute price.
        Stocks are lagged one day.

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
                 return self._getCoin(symbols[0], period, interval, market = "USD", datatype = datatype)
            else:
                return self._getStock(symbols[0], period, interval, adjusted, datatype)
        elif n > 1:
            if symbols[0] in self.getCoinNames():
                 return self._getCoins(symbols, period, interval, what = what)
            else:
                return self._getStocks(symbols, period, interval, adjusted, what)
        else:
            raise ValueError("Empty symbols input.")


