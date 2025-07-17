from functools import lru_cache
from typing import List, Union, Optional
import pandas as pd

from alphavantage.core.periods import Period, Interval
from alphavantage.utils.error_handling import RequestRetryHandler

class AlphaVantageAPI:
    """
    Class to interact with the Alpha Vantage API for retrieving historical data
    for stocks and cryptocurrencies
    """
    BASE_URL = "https://www.alphavantage.co/query"
    COIN_NAMES = ["BTC", "ETH", "DOGE", "AVAX", "SHIB", "LINK", "BCH", "LTC", "ETC", "AAVE"]

    def __init__(self, api_key: str):
        """
        Initializes the AlphaVantageAPI with the provided API key.

        Args:
            api_key (str): The API key for accessing Alpha Vantage API.
        """
        self.api_key = api_key
        self.retry_handler = RequestRetryHandler()

    # TODO add retry's by using requests.Session() ...
    #  Update: just tried it below...
    # def _make_request(self, params: dict) -> dict:
    #     """
    #     Makes a request to the Alpha Vantage API with the provided parameters.
    #
    #     Args:
    #         params (dict): Parameters for the API request.
    #
    #     Returns:
    #         dict: JSON response from the API.
    #
    #     Raises:
    #         HTTPError: If the API request fails.
    #     """
    #     params['apikey'] = self.api_key
    #     response = requests.get(self.BASE_URL, params=params, timeout=30)
    #     response.raise_for_status()
    #     return response.json()
    def _make_request(self, params: dict) -> dict:
        """
        Makes a request to the Alpha Vantage API with retry logic.

        Args:
            params (dict): Parameters for the API request.

        Returns:
            dict: JSON response from the API.

        Raises:
            HTTPError: If the API request fails after retries.
        """
        params['apikey'] = self.api_key
        response = self.retry_handler.request_with_retry("GET", self.BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_historical_data(self, symbol: str, period: Period, interval: Optional[Interval] = None,
                            adjusted: bool = True) -> pd.Series:
        """
        Retrieves historical data for the specified symbol and period.

        Args:
            symbol (str): The financial instrument symbol.
            period (Period): The period for the historical data.
            interval (Optional[Interval]): The interval for intraday data (required if period is INTRADAY).
            adjusted (bool): Whether to retrieve adjusted data (for non-crypto instruments).

        Returns:
            pd.Series: Time series data for the specified symbol and period.

        Raises:
            ValueError: If interval is not specified for intraday data.
        """
        is_crypto = symbol in self.COIN_NAMES
        function = self._get_function_name(is_crypto, period, adjusted)

        params = {
            "function": function,
            "symbol": symbol,
            "market": "USD" if is_crypto else None,
            "outputsize": "full",
        }

        if period == Period.INTRADAY:
            if not interval:
                raise ValueError("Interval must be specified for intraday data")
            params['interval'] = interval.value

        if not is_crypto and period != Period.INTRADAY:
            params['adjusted'] = 'true' if adjusted else 'false'

        data = self._make_request(params)
        return self._parse_time_series_data(data, period, is_crypto, adjusted)

    @staticmethod
    def _get_function_name(is_crypto: bool, period: Period, adjusted: bool) -> str:
        """
        Determines the function name for the API request based on the instrument type, period, and adjustment.

        Args:
            is_crypto (bool): Whether the instrument is a cryptocurrency.
            period (Period): The period for the historical data.
            adjusted (bool): Whether to retrieve adjusted data.

        Returns:
            str: The function name for the API request.
        """
        if is_crypto:
            base = "CRYPTO" if period == Period.INTRADAY else "DIGITAL_CURRENCY"
            return f"{base}_{period.value.upper()}"
        else:
            base = "TIME_SERIES"
            if period != Period.INTRADAY and adjusted:
                return f"{base}_{period.value.upper()}_ADJUSTED"
            return f"{base}_{period.value.upper()}"

    @staticmethod
    def _parse_time_series_data(data: dict, period: Period, is_crypto: bool, adjusted: bool) -> pd.Series:
        """
        Parses the time series data from the API response.

        Args:
            data (dict): The API response data.
            period (Period): The period for the historical data.
            is_crypto (bool): Whether the instrument is a cryptocurrency.
            adjusted (bool): Whether to retrieve adjusted data.

        Returns:
            pd.Series: The parsed time series data.
        """
        # print(data.keys())
        time_series_key = \
            [key for key in data.keys() if "Time Series" in key or "Time Series (Digital Currency Daily)" in key][0]
        df = pd.DataFrame.from_dict(data[time_series_key], orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.astype(float)

        if is_crypto:
            price_column = '4. close'
        else:
            price_column = '5. adjusted close' if adjusted and period != Period.INTRADAY else '4. close'

        series = df[price_column]
        series.sort_index(inplace=True)
        return series

    @lru_cache(maxsize=None)
    def get_coin_names(self) -> List[str]:
        """
        Retrieves the list of supported cryptocurrency names.

        Returns:
            List[str]: List of supported cryptocurrency names.
        """
        return self.COIN_NAMES

    def get_assets(self, symbols: Union[str, List[str]], period: Period,
                   interval: Optional[Interval] = None, adjusted: bool = True) -> pd.DataFrame:
        """
        Retrieves historical data for multiple symbols and combines them into a DataFrame.

        Args:
            symbols (Union[str, List[str]]): A single symbol or a list of symbols.
            period (Period): The period for the historical data.
            interval (Optional[Interval]): The interval for intraday data (required if period is INTRADAY).
            adjusted (bool): Whether to retrieve adjusted data.

        Returns:
            pd.DataFrame: DataFrame containing time series data for the specified symbols.

        Raises:
            ValueError: If symbols input is empty.
        """
        symbols = [symbols] if isinstance(symbols, str) else symbols
        if not symbols:
            raise ValueError("Empty symbols input.")

        dfs = []
        for symbol in symbols:
            series = self.get_historical_data(symbol, period, interval, adjusted)
            dfs.append(series)

        result = pd.concat(dfs, axis=1)
        result.columns = symbols
        result = result.dropna()
        return result
