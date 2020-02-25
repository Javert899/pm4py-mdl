import pandas as pd
from datetime import datetime
import pytz


def get_dt_from_string(dt):
    """
    If the date is expressed as string, do the conversion to a datetime.datetime object

    Parameters
    -----------
    dt
        Date (string or datetime.datetime)

    Returns
    -----------
    dt
        Datetime object
    """
    if type(dt) is str:
        return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")

    return dt


def apply(dataframe, dt1, dt2, parameters=None):
    if parameters is None:
        parameters = {}

    dt1 = get_dt_from_string(dt1)
    dt2 = get_dt_from_string(dt2)
    #dt1 = dt1.replace(tzinfo=pytz.utc)
    #dt2 = dt2.replace(tzinfo=pytz.utc)
    #dt1 = pd.to_datetime(dt1, utc=True)
    #dt2 = pd.to_datetime(dt2, utc=True)

    dataframe = dataframe[dataframe["event_timestamp"] > dt1]
    dataframe = dataframe[dataframe["event_timestamp"] < dt2]

    return dataframe
