from enum import Enum


class Period(Enum):
    """ Enumeration for different periods for historical data.
    """
    INTRADAY = "intraday"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class Interval(Enum):
    """
    Enumeration for different intervals for intraday data.
    """
    ONE_MIN = "1min"
    FIVE_MIN = "5min"
    FIFTEEN_MIN = "15min"
    THIRTY_MIN = "30min"
    SIXTY_MIN = "60min"