#!/usr/bin/python
# coding=utf-8
"""
@name shell执行器
@author jiangbing
@version 1.0.0
@update_time 2018-06-25
@comment 20180625 V1.0.0  jiangbing 新建
"""
import sys
import getopt
import datetime
import time
import os
import configparser
from utils.DBUtil import MysqlUtil
from utils.ProcUtil import ProcUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64
import utils.FuncUtil as fu

# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/RunSh.conf"
CONF = None
MYSQL = None
# 流水号
LSH = None
# 个性参数
VAR = {}
# 调度类型
TYPE = None
# 文件路径
FILE = None
# 程序名称
# COMP_NAME = None
# 日志
LOGGER = None
LOG_FILE = None
# 宏
MACRO = None


def show_help():
    """指令帮助"""
    print("""
    -t          调度类型
    -v          个性参数
    -l          流水号
    --file      文件路径
    """)
    sys.exit()


def validate_input():
    """验证参数"""
    if TYPE is None:
        print("please input -t")
        LOGGER.info("please input -t")
        sys.exit(1)
    if LSH is None:
        print("please input -l")
        LOGGER.info("please input -l")
        sys.exit(1)
    if FILE is None:
        print("please input --file")
        LOGGER.info("please input --file")
        sys.exit(1)


def init_param():
    """初始化参数"""
    # -t 1 -l 1 -r 20180307 -f /usr/local/source_code/dsc/json/load_jzjy_1.json
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:n:v:l:", ["help", "type=", "comp_name=", "var=", "lsh=", "file="])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        show_help()
        sys.exit(1)

    for name, value in opts:
        if name in ("-h", "--help"):
            show_help()
        if name in ("-t", "--type"):
            global TYPE
            TYPE = value
        if name in ("-v", "--var"):
            if value != "":
                global VAR
                tmp = value.split("&")
                for item in tmp:
                    t = item.split("=")
                    if t[1] is not None and t[1] != "":
                        VAR[t[0]] = t[1]
        if name in ("-l", "--lsh"):
            global LSH
            LSH = value
        if name in ("--file",):
            global FILE
            FILE = value
    validate_input()


def init_dataflow_macro():
    """初始化宏定义参数"""
    sql = "select * from t_dataflow_macro_def"
    res = MYSQL.query(sql, ())
    if len(res) > 0:
        global MACRO
        MACRO = {}
        for item in res:
            MACRO[item["CODE"]] = "%s;%s;%s" % (item["RESOURCE"], item["HSMC"], item["CSZ"])
    LOGGER.info("init_dataflow_macro: %s" % MACRO)


def deal_node_param(nodeParam):
    """解析宏定义参数"""
    param = ""
    for key in nodeParam:
        LOGGER.info("nodeParam[key]: %s" % nodeParam[key])
        if str(nodeParam[key]).startswith("@"):
            LOGGER.info("MACRO: %s,MACRO[nodeParam[key]]:%s" % (MACRO, MACRO[nodeParam[key]]))
            if MACRO is not None and MACRO[nodeParam[key]] is not None:
                v = nodeParam[key]
                tmp = MACRO[nodeParam[key]].split(";")
                if tmp[0] == "-l":
                    v = LSH
                else:
                    if tmp[0] in VAR:
                        v = VAR[tmp[0]]
                if v is None or v == "":
                    param += "%s=%s&" % (key, "")
                elif tmp[1] is None or tmp[1] == "":
                    param += "%s=%s&" % (key, v)
                else:
                    param += "%s=%s&" % (key, fu.fu_exec(v, tmp[1], tmp[2], MYSQL))
            else:
                param += "%s=%s&" % (key, nodeParam[key])
        else:
            param += "%s=%s&" % (key, nodeParam[key])
    param = param.rstrip("&")
    LOGGER.info("xml nodeParam: %s" % param)
    return param


def start_proc_logs(PROC_NAME, PROC_DESC):
    """记录程序日志"""
    sql = """
    insert into t_etl_proc_log(
    `DETAIL_LOG_ID`,
    `PROC_DESC`,
    `PROC_NAME`,
    `STAT_DATE`,
    `START_TIME`,
    `STATUS`,
    `RUN_TYPE`
    )values(%s,%s,%s,%s,%s,%s,%s)
    """
    rq = 0
    if "RQ" in VAR:
        rq = VAR["RQ"]
    elif "rq" in VAR:
        rq = VAR["rq"]
    LOGGER.info(sql % (LSH, PROC_DESC, PROC_NAME, rq, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1, TYPE))
    id = MYSQL.execute_sql(sql, (LSH, PROC_DESC, PROC_NAME, rq, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1, TYPE))
    return id


def end_proc_logs(ID, start, STATUS):
    """修改程序日志"""
    COST_TIME = int(time.time()) - start
    sql = "UPDATE t_etl_proc_log SET END_TIME=%s,COST_TIME=%s,STATUS=%s WHERE ID=%s"
    LOGGER.info(sql % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, STATUS, ID))
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, STATUS, ID))
    if STATUS == 3 or STATUS == 4:
        end_dataflow_logs(STATUS)
    else:
        end_dataflow_logs()


def update_proc_logs(ID, PROCESS_ID):
    """修改程序日志"""
    sql = "UPDATE t_etl_proc_log SET PROCESS_ID=%s WHERE ID=%s"
    LOGGER.info(sql % (PROCESS_ID, ID))
    MYSQL.execute_sql(sql, (PROCESS_ID, ID))


def is_stop():
    """是否终止执行程序"""
    sql = "SELECT ID FROM t_etl_proc_log WHERE DETAIL_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=4"
    LOGGER.info(sql % (LSH, TYPE))
    res = MYSQL.query(sql, (LSH, TYPE))
    return len(res) > 0


def end_dataflow_logs(STATUS=None):
    """修改节点日志"""
    if STATUS is None:
        sql = "UPDATE t_etl_dataflow_logs SET LOG_PATH=%s WHERE ID=%s"
        LOGGER.info(sql % (LOG_FILE, LSH))
        MYSQL.execute_sql(sql, (LOG_FILE, LSH))
    else:
        sql = "UPDATE t_etl_dataflow_logs SET STATUS=%s, LOG_PATH=%s  WHERE ID=%s"
        LOGGER.info(sql % (STATUS, LOG_FILE, LSH))
        MYSQL.execute_sql(sql, (STATUS, LOG_FILE, LSH))


def get_cmd():
    """sh执行命令"""
    cmd = "%s %s \"%s\"" % (CONF.get("conf", "run_bin"), FILE, deal_node_param(VAR))
    LOGGER.info(cmd)
    return cmd


def read_config():
    # 读取配置文件
    global DB_CONF, CONF, LOGGER, LOG_FILE
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH)
    LOG_FILE = (sys.path[0] + "/" + CONF.get("conf", "log_file")) % time.strftime("%Y%m%d", time.localtime())
    LOGGER = Logger(LOG_FILE).logger
    # 连接数据库
    global MYSQL
    MYSQL = MysqlUtil(
        DB_CONF.get("db", "host"),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get("db", "user"))),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get("db", "password"))),
        DB_CONF.get("db", "database"),
        DB_CONF.get("db", "port")
    )
    init_dataflow_macro()


def main():
    if FILE is None:
        return
    tmp = str(FILE).split("/")
    # id = start_proc_logs(tmp[len(tmp) - 1], COMP_NAME)
    id = start_proc_logs(tmp[len(tmp) - 1], tmp[len(tmp) - 1])
    start = int(time.time())

    def succ(pid, returncode, outs, errs):
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        end_proc_logs(id, start, 2)

    def fail(pid, returncode, outs, errs):
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        if returncode == -9:
            end_proc_logs(id, start, 4)
        else:
            end_proc_logs(id, start, 3)

    def before(pid):
        # 进程运行前操作
        update_proc_logs(id, pid)

    if os.path.exists(FILE):
        try:
            ProcUtil().single_pro(get_cmd(), succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_proc_logs(id, start, 3)
    else:
        # 执行失败记录程序日志
        end_proc_logs(id, start, 3)
        LOGGER.info("File is no exists :%s" % FILE)


if __name__ == '__main__':
    read_config()
    init_param()
    main()
