import pandas as pd
from pandas import Interval, Timestamp

from datetime import datetime


class DateInterval:
    def __init__(self, start: datetime, end: datetime):
        if start > end:
            raise ValueError('Start date must be before end date')
        self.start = start
        self.end = end

    @staticmethod
    def pandas_type_repr():
        return pd.Interval
