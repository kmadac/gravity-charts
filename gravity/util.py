__author__ = 'kmadac'


def get_prev_minute_timestamp(timestamp_sec):
    """
    Returns timestamp of minute to which timestamp_sec parameter belongs.
    It means that if timestamp_sec argument is 1407838210 (10h10m10s), function returns 1407838200(10h10m0s)
    """
    return timestamp_sec / 60 * 60