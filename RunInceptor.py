#!/usr/bin/python
# coding=utf-8
"""
@name inceptor执行器
@author jiangbing
@version 1.0.0
@update_time 2018-11-20
@comment 20181120 V1.0.0  jiangbing 新建
"""
import sys
import getopt
import time
import os
import json
import codecs
import multiprocessing
import configparser
from utils.DBUtil import MysqlUtil
from utils.LogUtil import Logger
from utils.ProcUtil import ProcUtil
from pyDes import des, PAD_PKCS5
import base64
import datetime

# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/RunInceptor.conf"
CONF = None
MYSQL = None
# 流水号
LSH = None
# 调度类型
TYPE = None
# 文件路径
FILE = None
# 个性参数
VAR = {}
# 日志
LOGGER = None
LOG_FILE = None
# 目录
WORKPATH = None
# 主机
HOSTNODE = None


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
    # -t 1 -l 174 -v RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/impala/conf/tran_jzjysj.json
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:", ["help", "type=", "var=", "lsh=", "file="])
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


def validate_proc_input(params):
    for param in params:
        if param["need"] == "1":
            if param["param"] not in VAR:
                print("please input %s, %s" % (param["name"], param["comment"]))
                LOGGER.info("please input %s, %s" % (param["name"], param["comment"]))
                sys.exit(1)


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


def fileExists(i_path):
    """判断执行程序存在与否"""
    return os.path.exists(i_path)


def get_hostnode():
    """获取hive主机"""
    sql = """
        SELECT HOST_IP,USER_NAME,PASSWORD,DB_LINK FROM t_srm_datasource WHERE TYPE=2 LIMIT 1
    """
    LOGGER.info(sql)
    res = MYSQL.query(sql, ())
    if len(res) > 0:
        return res[0]
    else:
        return None


def getRunCmd(proc):
    """组织运行程序的命令串"""
    param = ""
    for item in VAR:
        param += "--hivevar %s=%s " % (item, VAR[item])
    # beeline -u "jdbc:hive2://192.168.0.87:10000/default" -n user -p 123456 -hivevar RQ=20180808 -hivevar KSRQ=20180808  -f /home/bigdata/script/hive/dscc_aboss/load_tcgzhdy.sql

    run_bin = CONF.get("conf", "run_bin")
    if run_bin.count("%s") == 1:
        run_bin = run_bin % HOSTNODE["DB_LINK"]
    elif run_bin.count("%s") == 3:
        LOGGER.info("%s,%s,%s,%s" % (HOSTNODE["DB_LINK"], HOSTNODE["USER_NAME"], DB_CONF.get("conf", "des_key"), HOSTNODE["PASSWORD"]))
        run_bin = run_bin % (HOSTNODE["DB_LINK"], HOSTNODE["USER_NAME"], des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(HOSTNODE["PASSWORD"])).decode('utf-8', errors='ignore'))
    cmd = '%s %s -f %s' % (run_bin, param, WORKPATH + "/" + proc)
    LOGGER.info(cmd)
    return cmd


def excute(definitions, exception):
    """执行主程序入口"""
    for definition in definitions:
        LOGGER.info("definition:%s" % len(definition))
        flag = True
        if len(definition) == 1:
            LOGGER.info("definition:%s" % definition[0])
            if "PROC" in VAR and VAR["PROC"] != "":
                proc_name = definition[0]["file"].split("/")
                if VAR["PROC"].find(proc_name[len(proc_name) - 1]) == -1:
                    continue
            flag = excute_singleprocess(definition[0]["file"], definition[0]["desc"])
        elif len(definition) > 1:
            flag = excute_multiprocess(definition)
        else:
            pass

        if flag is False and exception == "break":
            sys.exit()


def excute_singleprocess(proc, proc_desc):
    """串行执行主方法"""
    LOGGER.info("proc:%s, proc_desc:%s" % (proc, proc_desc))
    if is_stop() is True:
        return True
    flag = False
    abPath = WORKPATH + '/' + proc
    # 开始记录程序日志
    proc_name = proc.split("/")
    id = start_proc_logs(proc_name[len(proc_name) - 1], proc_desc)
    start = int(time.time())
    if fileExists(abPath) is not True:
        # 执行失败记录程序日志
        end_proc_logs(id, start, 3)
        LOGGER.info("File is no exists :%s" % abPath)
        return

    def succ(pid, returncode, outs, errs):
        # 执行成功记录程序日志
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        end_proc_logs(id, start, 2)

    def fail(pid, returncode, outs, errs):
        # 执行失败记录程序日志
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        end_proc_logs(id, start, 3)

    def before(pid):
        # 进程运行前操作
        update_proc_logs(id, pid)

    try:
        returncode = ProcUtil().single_pro(getRunCmd(proc), succ, fail, before)
    except Exception as e:
        LOGGER.info("error: %s" % e)
        returncode = 1
        end_proc_logs(id, start, 3)

    if returncode == 0:
        flag = True
    return flag


def excute_multiprocess(definition):
    """并行执行主方法"""
    flag = True
    results = []
    parallel_num = int(CONF.get("conf", "parallel_num"))
    LOGGER.info("parallel_num :%s" % parallel_num)
    pool = multiprocessing.Pool(processes=parallel_num)
    for proc in definition:
        # LOGGER.info("proc :%s" % proc)
        proc_file = proc["file"]
        proc_name = proc_file.split("/")
        if "PROC" in VAR and VAR["PROC"] != "":
            if VAR["PROC"].find(proc_name[len(proc_name) - 1]) == -1:
                continue
        results.append(pool.apply_async(execRunCmd_multi, args=(proc_name, proc["file"], proc["desc"])))
    pool.close()
    pool.join()
    for res in results:
        if res.get() != 0:
            return False
    return flag


def execRunCmd_multi(proc_name, proc_file, proc_desc):
    """并行执行子程序"""
    if is_stop() is True:
        return 0
    id = start_proc_logs(proc_name[len(proc_name) - 1], proc_desc)
    abPath = WORKPATH + '/' + proc_file
    start = int(time.time())
    if fileExists(abPath) is not True:
        # 执行失败记录程序日志
        end_proc_logs(id, start, 3)
        LOGGER.info("File is no exists :%s" % abPath)
        return 1

    def succ(pid, returncode, outs, errs):
        # 执行成功记录程序日志
        LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
        LOGGER.info("outs: %s" % outs)
        LOGGER.info("errs: %s" % errs)
        end_proc_logs(id, start, 2)

    def fail(pid, returncode, outs, errs):
        # 执行失败记录程序日志
        LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
        LOGGER.info("outs: %s" % outs)
        LOGGER.info("errs: %s" % errs)
        if returncode == -9:
            end_proc_logs(id, start, 4)
        else:
            end_proc_logs(id, start, 3)

    def before(pid):
        # 进程运行前操作
        update_proc_logs(id, pid)

    try:
        returncode = ProcUtil().single_pro(getRunCmd(proc_file), succ, fail, before)
    except Exception as e:
        LOGGER.info("error: %s" % e)
        returncode = 1
        end_proc_logs(id, start, 3)
    return returncode


def getJsonStr(path):
    """根据文件路径获取json文件"""
    with codecs.open(path, 'r', 'utf-8') as json_temp:
        load_dict = json.loads(json_temp.read())
        return load_dict


def read_config():
    # 读取配置文件
    global DB_CONF, CONF, LOGGER, LOG_FILE
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    # 读取配置文件
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
    try:
        global HOSTNODE
        HOSTNODE = get_hostnode()

        # 获取rq参数，json文件路径
        json_p = FILE
        LOGGER.info(json_p)
        json_sql = getJsonStr(json_p)
        LOGGER.info("3")
        validate_proc_input(json_sql["params"])
        LOGGER.info("4")
        global WORKPATH
        WORKPATH = sys.path[0]
        LOGGER.info("5")
        definitions = json_sql["inceptor_files"]
        LOGGER.info("6")
        exception = json_sql["exception"]
        LOGGER.info("7")
        # 主调程序
        excute(definitions, exception)
    except Exception as e:
        LOGGER.info("error: %s" % e)
        end_dataflow_logs(3)


if __name__ == '__main__':
    read_config()
    init_param()
    main()
