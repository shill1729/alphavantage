import os
import time
import finnhub
import pandas as pd
import datetime as dt
from finnhub import Client
from pandas import DataFrame


# TODO: check that this has method .quote and implement other clients
def get_quote_from_client(symbols, client: Client):
    """

    :param symbols: list of tickers
    :param client: finnhub.Client(api_key) class.
    :return:
    """
    quotes = []
    for symbol in symbols:
        quote = client.quote(symbol)["c"]
        quotes.append(quote)
        time.sleep(1 / len(symbols))
    return quotes

# TODO: any other finnhub data we want?
def update_with_quotes(prices: DataFrame):
    """

    :param prices:
    :return:
    """
    today = dt.date.today()
    symbols = prices.columns
    client = finnhub.Client(api_key=os.getenv("FINNHUB_KEY"))
    quotes = get_quote_from_client(symbols, client)
    new_row = pd.DataFrame([quotes], columns=symbols, index=[today])
    if today in prices.index:
        prices.loc[today] = quotes
    else:
        prices = pd.concat([prices, new_row])
    return prices