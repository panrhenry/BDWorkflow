#!/usr/bin/python
# coding=utf-8
"""
@name kettle执行器
@author jiangbing
@version 1.0.0
@update_time 2018-06-25
@comment 20180625 V1.0.0  jiangbing 新建
"""
import sys
import getopt
import datetime
import time
import configparser
from utils.DBUtil import MysqlUtil
from utils.LogUtil import Logger
from utils.ProcUtil import ProcUtil
from pyDes import des, PAD_PKCS5
import base64

# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/RunKettle.conf"
CONF = None
MYSQL = None
# 流水号
LSH = None
# 个性参数
VAR = {}
# 调度类型
TYPE = None
# 作业名（外部）
PROC_NAME = None
# 作业描述
PROC_DESC = None
# 作业路径
PROC_PATH = None
# 作业名（内部）
JOB_NAME = None
# 日志
LOGGER = None
LOG_FILE = None


def show_help():
    """指令帮助"""
    print("""
    -t          调度类型
    -v          个性参数
    -l          流水号
    --job       作业名（外部）
    --path      作业路径
    --desc      作业描述   
    --job_name  作业名（内部）   
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
    if PROC_NAME is None:
        print("please input --job")
        LOGGER.info("please input --job")
        sys.exit(1)
    if PROC_PATH is None:
        print("please input --path")
        LOGGER.info("please input --path")
        sys.exit(1)
    if PROC_DESC is None:
        print("please input --desc")
        LOGGER.info("please input --desc")
        sys.exit(1)
    if JOB_NAME is None:
        print("please input --job_name")
        LOGGER.info("please input --job_name")
        sys.exit(1)


def init_param():
    """初始化参数"""
    # -t 1 -l 1 -r 20180307 -f /usr/local/source_code/dsc/json/load_jzjy_1.json
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:", ["help", "type=", "var=", "lsh=", "job=", "path=", "desc=", "job_name="])
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
        if name in ("--job",):
            global PROC_NAME
            PROC_NAME = value
        if name in ("--path",):
            global PROC_PATH
            PROC_PATH = value
        if name in ("--desc",):
            global PROC_DESC
            PROC_DESC = value
        if name in ("--job_name",):
            global JOB_NAME
            JOB_NAME = value
    validate_input()


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
    sql = "UPDATE t_etl_proc_log SET END_TIME=%s,COST_TIME=%s WHERE ID=%s"
    LOGGER.info(sql % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, ID))
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, ID))
    sql = "UPDATE t_etl_proc_log SET STATUS=%s WHERE ID=%s AND STATUS NOT IN (3,4)"
    LOGGER.info(sql % (STATUS, ID))
    MYSQL.execute_sql(sql, (STATUS, ID))
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
        sql = "UPDATE t_etl_dataflow_logs SET LOG_PATH=%s WHERE ID=%s"
        LOGGER.info(sql % (LOG_FILE, LSH))
        MYSQL.execute_sql(sql, (LOG_FILE, LSH))
        sql = "UPDATE t_etl_dataflow_logs SET STATUS=%s WHERE ID=%s AND STATUS NOT IN (3,4)"
        LOGGER.info(sql % (STATUS, LSH))
        MYSQL.execute_sql(sql, (STATUS, LSH))


def get_kettle_hostnode():
    """获取kettle主机"""
    sql = """
        SELECT HOST_IP,USER_NAME,PASSWORD,HOST_PORT FROM t_srm_hostnode WHERE TYPE=2 AND ISDEFAULT=1 LIMIT 1
    """
    LOGGER.info(sql)
    res = MYSQL.query(sql, ())
    if len(res) > 0:
        return res[0]
    else:
        return None


def get_cmd():
    """kettle执行命令"""
    param_str = ""
    for item in VAR:
        param_str += "%s=%s&" % (item, VAR[item])
    param_str = param_str.rstrip("&")
    cmd = "%s /rep:%s /user:%s /pass:%s /job:%s /dir:%s /param:%s /level:Basic" % (
        CONF.get("conf", "run_bin"), CONF.get("conf", "rep"), CONF.get("conf", "user"), CONF.get("conf", "password"), JOB_NAME, PROC_PATH, param_str)
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


def main():
    hostnode = get_kettle_hostnode()
    # 开始记录程序日志
    id = start_proc_logs(PROC_NAME, PROC_DESC)
    start = int(time.time())

    def succ(returncode, outs, errs):
        # 执行成功记录节点日志
        LOGGER.info("ssh_exec_cmd returncode: %s" % returncode)
        LOGGER.info("ssh_exec_cmd out: %s" % outs)
        LOGGER.info("ssh_exec_cmd errs: %s" % errs)
        end_proc_logs(id, start, 2)

    def fail(returncode, outs, errs):
        # 执行失败记录节点日志
        LOGGER.info("ssh_exec_cmd returncode: %s" % returncode)
        LOGGER.info("ssh_exec_cmd out: %s" % outs)
        LOGGER.info("ssh_exec_cmd errs: %s" % errs)
        if returncode == -9:
            end_proc_logs(id, start, 4)
        else:
            end_proc_logs(id, start, 3)

    def before(pid):
        # 进程运行前操作
        update_proc_logs(id, pid)

    ProcUtil().ssh_exec_cmd(
        hostnode["HOST_IP"],
        hostnode["HOST_PORT"],
        hostnode["USER_NAME"],
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(hostnode["PASSWORD"])),
        get_cmd(),
        succ,
        fail
    )


if __name__ == '__main__':
    read_config()
    init_param()
    main()
