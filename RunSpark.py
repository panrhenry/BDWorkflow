#!/usr/bin/python
# coding=utf-8
"""
@name spark执行器
@author jiangbing
@version 1.0.0
@update_time 2018-06-25
@comment 20180625 V1.0.0  jiangbing 新建
"""
import sys
import getopt
import datetime
import time
import json
import codecs
import configparser
from utils.DBUtil import MysqlUtil
from utils.ProcUtil import ProcUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64
import multiprocessing

# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/RunSpark.conf"
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
# 日志
LOGGER = None
LOG_FILE = None
# 开始日期
KSRQ = None
# 结束日期
JSRQ = None


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
    if KSRQ is None:
        print("please input ksrq")
        LOGGER.info("please input ksrq")
        sys.exit(1)
    if JSRQ is None:
        print("please input jsrq")
        LOGGER.info("please input jsrq")
        sys.exit(1)


def init_param():
    """初始化参数"""
    # -t 1 -l 192 -v KSRQ=20180101&JSRQ=20180101 --file=/home/bigdata/BDWorkflow/comp/spark/conf/spark_jzjyrzqs.json
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
            global KSRQ, JSRQ
            if "KSRQ" in VAR:
                KSRQ = VAR["KSRQ"]
            if "ksrq" in VAR:
                KSRQ = VAR["ksrq"]
            if "JSRQ" in VAR:
                JSRQ = VAR["JSRQ"]
            if "jsrq" in VAR:
                JSRQ = VAR["jsrq"]
        if name in ("-l", "--lsh"):
            global LSH
            LSH = value
        if name in ("--file",):
            global FILE
            FILE = value
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
    LOGGER.info(sql % (LSH, PROC_DESC, PROC_NAME, KSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1, TYPE))
    id = MYSQL.execute_sql(sql, (LSH, PROC_DESC, PROC_NAME, KSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1, TYPE))
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


def update_proc_logs_appname(ID, SPARK_NAME):
    """修改程序日志"""
    sql = "UPDATE t_etl_proc_log SET SPARK_NAME=%s WHERE ID=%s"
    LOGGER.info(sql % (SPARK_NAME, ID))
    MYSQL.execute_sql(sql, (SPARK_NAME, ID))


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


def get_cmd(proc):
    """spark执行命令"""
    # spark-submit --class com.apex.id.IdsRunner --master yarn --deploy-mode cluster --executor-memory 2G --executor-cores 5 --num-executors 4 --driver-memory 2g /home/spark_test/centerTrade-1.0-SNAPSHOT.jar "FILE=/home/spark_test/config.properties&KSRQ=20170101&JSRQ=20170110&KHH=041100000011"

    env_conf = proc["env_conf"]
    other_conf = ""
    for key in CONF[env_conf]:
        other_conf += "--%s %s " % (key, CONF[env_conf][key])
    if proc["jar"].startswith("/"):
        jar = proc["jar"]
    else:
        jar = "%s/%s" % (sys.path[0], proc["jar"])
    if proc["properties_file"].startswith("/"):
        properties_file = proc["properties_file"]
    else:
        properties_file = "%s/%s" % (sys.path[0], proc["properties_file"])
    param_str = ""
    for item in VAR:
        param_str += "%s=%s&" % (item, VAR[item])
    run_bin = CONF.get("conf", "run_bin") % ""
    if "properties_file" in proc and proc["properties_file"] != "":
        run_bin = CONF.get("conf", "run_bin") % ("," + properties_file)
    cmd = "%s %s --name %s %s %s \"%sFILE=%s&LOGID=%s\"" % (run_bin, proc["opt"], proc["app_name"], other_conf, jar, param_str, properties_file, LSH)
    LOGGER.info(cmd)
    return cmd


def getJsonStr(path):
    """根据文件路径获取json文件"""
    with codecs.open(path, 'r', 'utf-8') as json_temp:
        load_dict = json.loads(json_temp.read())
        return load_dict


def validate_proc_input(params):
    for param in params:
        if param["need"] == "1":
            if param["param"] not in VAR:
                LOGGER.info("please input %s, %s" % (param["name"], param["comment"]))
                sys.exit(1)


def excute_singleprocess(proc):
    """串行执行主方法"""
    if is_stop() is True:
        return True
    # 开始记录程序日志
    proc_file = ""
    if "jar" in proc and proc["jar"] != "":
        proc_file = proc["jar"]
    proc_name = proc_file.split("/")
    id = start_proc_logs(proc_name[len(proc_name) - 1], proc["desc"])
    start = int(time.time())

    def succ(pid, returncode, outs, errs):
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        # 执行成功记录节点日志
        end_proc_logs(id, start, 2)

    def fail(pid, returncode, outs, errs):
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        # 执行失败记录节点日志
        if returncode == -9:
            end_proc_logs(id, start, 4)
        else:
            end_proc_logs(id, start, 3)

    def before(pid):
        # 进程运行前操作
        update_proc_logs(id, pid)

    proc["app_name"] = proc["app_name"] + "_" + str(id)
    update_proc_logs_appname(id, proc["app_name"])
    try:
        returncode = ProcUtil().single_pro(get_cmd(proc), succ, fail, before)
    except Exception as e:
        LOGGER.info("error: %s" % e)
        returncode = 1
        end_proc_logs(id, start, 3)
    return returncode == 0


def execRunCmd_multi(proc):
    """串行执行主方法"""
    if is_stop() is True:
        return 0
    proc_name = proc["jar"].split("/")
    id = start_proc_logs(proc_name[len(proc_name) - 1], proc["desc"])
    start = int(time.time())

    def succ(pid, returncode, outs, errs):
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        # 执行成功记录节点日志
        end_proc_logs(id, start, 2)

    def fail(pid, returncode, outs, errs):
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        # 执行失败记录节点日志
        if returncode == -9:
            end_proc_logs(id, start, 4)
        else:
            end_proc_logs(id, start, 3)

    def before(pid):
        # 进程运行前操作
        update_proc_logs(id, pid)

    # get_cmd()
    proc["app_name"] = proc["app_name"] + "_" + id
    update_proc_logs_appname(id, proc["app_name"])
    try:
        returncode = ProcUtil().single_pro(get_cmd(proc), succ, fail, before)
    except Exception as e:
        LOGGER.info("error: %s" % e)
        returncode = 1
        end_proc_logs(id, start, 3)
    return returncode


def excute_multiprocess(definition):
    """并行执行主方法"""
    flag = True
    results = []
    parallel_num = int(CONF.get("conf", "parallel_num"))
    LOGGER.info("parallel_num :%s" % parallel_num)
    pool = multiprocessing.Pool(processes=parallel_num)
    for proc in definition:
        results.append(pool.apply_async(execRunCmd_multi, args=(proc,)))
    pool.close()
    pool.join()
    for res in results:
        if res.get() != 0:
            return False
    return flag


def excute(definitions, exception):
    """执行主程序入口"""
    for definition in definitions:
        flag = True
        if len(definition) == 1:
            flag = excute_singleprocess(definition[0])
        elif len(definition) > 1:
            flag = excute_multiprocess(definition)
        else:
            pass

        if flag is False and exception == "break":
            sys.exit()


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
        info = getJsonStr(FILE)
        validate_proc_input(info["params"])
        definitions = info["spark_files"]
        exception = info["exception"]

        # 主调程序
        excute(definitions, exception)
    except Exception as e:
        LOGGER.info("error: %s" % e)
        end_dataflow_logs(3)


if __name__ == '__main__':
    read_config()
    init_param()
    main()
