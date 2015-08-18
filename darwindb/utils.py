import pytz
from datetime import datetime, timedelta

def timezone_for_date_and_time(d, t):
    tz = pytz.timezone("Europe/London")
    dt = pytz.utc.localize(datetime.combine(d, t))
    if dt.astimezone(tz).utcoffset().seconds / 3600 == 1:
        # Think the timezone below is wrong? Read the whole of the linked file, then think again.
        # https://github.com/eggert/tz/blob/master/etcetera
        return pytz.timezone("Etc/GMT-1")
    else:
        return pytz.timezone("Etc/GMT")


