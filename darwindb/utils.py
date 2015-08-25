import pytz
from datetime import datetime, timedelta, date, time

def timezone_for_date_and_time(d, t):
    tz = pytz.timezone("Europe/London")
    dt = pytz.utc.localize(datetime.combine(d, t))
    if dt.astimezone(tz).utcoffset().seconds / 3600 == 1:
        # Think the timezone below is wrong? Read the whole of the linked file, then think again.
        # https://github.com/eggert/tz/blob/master/etcetera
        return pytz.timezone("Etc/GMT-1")
    else:
        return pytz.timezone("Etc/GMT")


def subtract_times(a, b):
    delta = datetime.combine(date.today(), a) - datetime.combine(date.today(), b)
    return (delta.days*24*60*60)+delta.seconds


def apply_date_and_tz_to_time(dated_time, tz, previous_time, this_time):
    delta = subtract_times(this_time, previous_time)
    if delta < -21600:
        # Crossed Midnight into the next day.
        d = (dated_time + timedelta(days=1)).date()

    elif delta < 64800:
        # Normal time.
        d = dated_time.date()

    else:
        # Delayed backwards over midnight into previous day.
        d = (dated_time + timedelta(days=-1)).date()

    return tz.localize(datetime.combine(d, this_time)).astimezone(pytz.utc)


def add_minutes_to_time(t, minutes):
    if minutes is None:
        return t

    d = datetime.combine(datetime.today().date(), t)
    d = d + timedelta(minutes=minutes)
    return d.time()


