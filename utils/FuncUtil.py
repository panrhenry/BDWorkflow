# coding=utf-8
"""
常用方法类
@author jiangbing
"""
import datetime


def fu_get_date(rq, days):
    """获取日期"""
    days = int(days)
    if days == 0:
        return rq
    rq_day = datetime.datetime.strptime(rq, '%Y%m%d')
    delta = datetime.timedelta(days=days)
    day = rq_day + delta
    return day.strftime('%Y%m%d')


def fu_get_jyr(rq, days, MYSQL):
    """获取交易日"""
    days = int(days)
    if days == 0:
        return rq
    sql = "SELECT F_GET_JYR_DATE(%s, %s) as d FROM DUAL"
    res = MYSQL.query(sql, (rq, days))
    if len(res) > 0:
        return res[0]["d"]
    else:
        return None


def fu_exec(value, name, param, MYSQL):
    if name == "fu_get_date":
        return fu_get_date(value, param)
    elif name == "fu_get_jyr":
        return fu_get_jyr(value, param, MYSQL)
    return value

