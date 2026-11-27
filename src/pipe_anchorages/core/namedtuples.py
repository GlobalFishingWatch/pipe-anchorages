import datetime


epoch = datetime.datetime.fromtimestamp(0, datetime.UTC)


def datetime_to_s(x):
    return (x - epoch).total_seconds()


_datetime_to_s = datetime_to_s


def s_to_datetime(x):
    return epoch + datetime.timedelta(seconds=x)


_s_to_datetime = s_to_datetime
