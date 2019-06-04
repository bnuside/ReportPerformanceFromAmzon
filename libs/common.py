import time


def get_cdate(x_day_before, today=None):
    if not today:
        today = time.time()
    day_seconds = 60 * 60 * 24
    target_timestamp = today - day_seconds * x_day_before
    target_date = time.localtime(target_timestamp)
    year = target_date.tm_year
    if target_date.tm_mon < 10:
        month = '0%s' % target_date.tm_mon
    else:
        month = target_date.tm_mon

    if target_date.tm_mday < 10:
        day = '0%s' % target_date.tm_mday
    else:
        day = target_date.tm_mday

    return '%s%s%s' % (year, month, day), '%s-%s-%s' % (year, month, day)
