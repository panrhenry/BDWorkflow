#!/usr/bin/python
# coding=utf-8
"""
@name 处理状态标识
@author chenss
@version 1.0.0
@update_time 2019-04-11
@comment 20191031 V1.0.0  chenshuangshui 新建
"""
import sys
import getopt
import datetime
import time
import configparser
from utils.DBUtil import MysqlUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64

# reload(sys)
# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
CONF = None
MYSQL = None
ORACLE = None
# 流水号
LSH = None
# 个性参数
VAR = None
# 调度类型
TYPE = None
# 操作类型
OPR_TYPE = None
# 数据工作流程名称
TASK_NAME = None
# 日期
EXEC_RQ = None
# 日志
LOGGER = None
LOG_FILE = None
# 主机
HOSTNODE = None
# 平台类型是否为TDH
IS_TDH = False


# 异常标志

def show_help():
    """指令帮助"""
    print("""
    --TASK_NAME        数据工作流程名称
    --EXEC_RQ             运行日期（如20181113）
        """)
    sys.exit()


def validate_input():
    """验证参数"""
    if TASK_NAME is None:
        print("please input --TASK_NAME")
        LOGGER.info("please input --TASK_NAME")
        sys.exit(1)
    if EXEC_RQ is None:
        print("please input --EXEC_RQ")
        LOGGER.info("please input --EXEC_RQ")
        sys.exit(1)


def init_param():
    """初始化参数"""
    # python RunDataFlowTaskFlag.py --TASK_NAME=任务名称 --EXEC_RQ=20190305
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:", ["help", "TASK_NAME=", "EXEC_RQ="])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        show_help()
        sys.exit(1)

    for name, value in opts:
        if name in ("-h", "--help"):
            show_help()
        if name in ("--TASK_NAME",):
            global TASK_NAME
            TASK_NAME = value
        if name in ("--EXEC_RQ",):
            global EXEC_RQ
            EXEC_RQ = value
    validate_input()


def read_config():
    # 读取配置文件
    global DB_CONF, CONF, LOGGER, LOG_FILE, CREATETBL_DIR, STORAGE_TYPE, HDFS_LOCATION, IS_TDH
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)

    global MYSQL
    MYSQL = MysqlUtil(
        DB_CONF.get("db", "host"),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get("db", "user"))),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
            base64.b64decode(DB_CONF.get("db", "password"))),
        DB_CONF.get("db", "database"),
        DB_CONF.get("db", "port")
    )

def deal_task_flag_logs(task_name,exec_rq):
    # 处理状态标识记录
    sql = "insert into pub_sys.t_dataflow_task_flag(TASK_NAME,RUN_DATE,DATA_DATE,RUN_STATUS)" \
          "values('%s','%s','%s','%s')"
    exc_sql = sql % (task_name,
                     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),exec_rq, '1')
    print(exc_sql)
    MYSQL.execute_sql(exc_sql, {})


def main():
    print(TASK_NAME)
    print(EXEC_RQ)
    deal_task_flag_logs(TASK_NAME,EXEC_RQ)


if __name__ == '__main__':
    read_config()
    init_param()
    main()
