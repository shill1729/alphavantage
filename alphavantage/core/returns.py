from pandas import DataFrame
import numpy as np

def get_log_returns(prices: DataFrame):
    log_returns = prices.apply(lambda x: np.diff(np.log(x)))
    return log_returns.dropna()

def get_arithmetic_returns(prices: DataFrame):
    log_returns = get_log_returns(prices)
    arithmetic_returns = np.exp(log_returns)-1
    return arithmetic_returns