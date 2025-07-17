from typing import Optional
from alphavantage.core.periods import Period, Interval

def compute_time_step(period: Period, interval: Optional[Interval] = None):
    """

    :param period:
    :param interval:
    :return:
    """
    if period == Period.DAILY:
        return 1 / 365
    elif period == Period.WEEKLY:
        return 1 / 52
    elif period == Period.MONTHLY:
        return 1 / 12
    elif period == Period.INTRADAY:
        if interval == Interval.ONE_MIN:
            return 1 / (24 * 60)
        elif interval == Interval.FIVE_MIN:
            return 5 / (24 * 60)
        elif interval == Interval.FIFTEEN_MIN:
            return 15 / (24 * 60)
        elif interval == Interval.THIRTY_MIN:
            return 30 / (24 * 60)
        elif interval == Interval.SIXTY_MIN:
            return 1 / 24
        else:
            raise ValueError("Invalid interval for intraday period")
    else:
        raise ValueError("Invalid period")