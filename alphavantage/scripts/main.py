if __name__ == "__main__":
    import os
    from alphavantage.api import AlphaVantageAPI
    from alphavantage.core import Period, Interval
    from alphavantage.core import get_log_returns, get_arithmetic_returns


    def main():
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not api_key:
            raise EnvironmentError("ALPHA_VANTAGE_API_KEY not set in environment.")

        av = AlphaVantageAPI(api_key)

        period = Period.INTRADAY
        interval = Interval.ONE_MIN
        symbols = ["GOTU", "GE"]

        print(f"Fetching historical price data for {symbols}...")
        prices = av.get_assets(symbols, period=period, interval=interval)
        print("Sample prices:")
        print(prices.tail())

        print("\nComputing log returns...")
        log_returns = get_log_returns(prices)
        print("Sample log returns:")
        print(log_returns.tail())

        print("\nMean log returns:")
        print(log_returns.mean())

        print("\nComputing arithmetic returns...")
        arithmetic = get_arithmetic_returns(prices)
        print("Sample arithmetic returns:")
        print(arithmetic.tail())

        print("\nMean arithmetic returns:")
        print(arithmetic.mean())


    if __name__ == "__main__":
        main()

