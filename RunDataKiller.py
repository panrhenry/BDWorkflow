#!/usr/bin/python
# coding=utf-8
"""
@name 手动终止数据流程程序
@author jiangbing
@version 1.0.0
@update_time 2018-12-28
@comment 20181228 V1.0.0  jiangbing 新建
"""
import sys
import getopt
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
CONF_PATH = sys.path[0] + "/conf/RunDataKiller.conf"
CONF = None
MYSQL = None
# 调度类型
TYPE = None
# 流水号
LSH = None
# 日志
LOGGER = None
LOG_FILE = None


def show_help():
    """指令帮助"""
    print("""
    -t          调度类型
    -l          流水号
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


def init_param():
    """初始化参数"""
    # -t 1 -l 174 -v RQ=20180101 --file=./conf/tran_jzjysj.json
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:l:", ["help", "type=", "lsh="])
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
        if name in ("-l", "--lsh"):
            global LSH
            LSH = value
    validate_input()


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


def get_dataflow_logs_1(JOB_LOG_ID):
    """获取调度执行节点日志"""
    sql = "SELECT ID,JOB_LOG_ID,PROCESS_ID,COMP_TYPE FROM t_etl_dataflow_logs WHERE JOB_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=1"
    LOGGER.info(sql % (JOB_LOG_ID, TYPE))
    res = MYSQL.query(sql, (JOB_LOG_ID, TYPE))
    LOGGER.info("res :%s" % res)
    if len(res) > 0:
        return res
    else:
        return None


def update_dataflow_logs_1(JOB_LOG_ID):
    """修改调度执行节点日志"""
    """status 状态：1 正在执行 2 执行完成 3 执行失败 4 手动终止"""
    sql = "SELECT START_TIME FROM t_etl_dataflow_logs WHERE JOB_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=1"
    res = MYSQL.query(sql, (JOB_LOG_ID, TYPE))
    if len(res) > 0:
        START_TIME = res[0]["START_TIME"]
        sql = """
        UPDATE t_etl_dataflow_logs SET STATUS=4,END_TIME=NOW(),COST_TIME=UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(START_TIME) WHERE JOB_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=1
        """
        LOGGER.info(sql % (JOB_LOG_ID, TYPE))
        MYSQL.execute_sql(sql, (JOB_LOG_ID, TYPE))


def get_dataflow_logs_2(ID):
    """获取手工执行节点日志"""
    sql = "SELECT ID,JOB_LOG_ID,PROCESS_ID,COMP_TYPE FROM t_etl_dataflow_logs WHERE ID=%s AND RUN_TYPE=%s AND JOB_LOG_ID=-1 AND STATUS=1 LIMIT 1"
    LOGGER.info(sql % (ID, TYPE))
    res = MYSQL.query(sql, (ID, TYPE))
    LOGGER.info("res :%s" % res)
    if len(res) > 0:
        return res[0]
    else:
        return None


def update_dataflow_logs_2(ID):
    """修改手工执行节点日志"""
    """status 状态：1 正在执行 2 执行完成 3 执行失败 4 手动终止"""
    #
    sql = "UPDATE t_etl_dataflow_logs SET STATUS=4,END_TIME=NOW(),COST_TIME=UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(START_TIME) WHERE ID=%s AND RUN_TYPE=%s AND JOB_LOG_ID=-1 AND STATUS=1"
    LOGGER.info(sql % (ID, TYPE))
    MYSQL.execute_sql(sql, (ID, TYPE))


def get_dataflow_logs_0(ID):
    """获取手工执行节点日志"""
    sql = "SELECT ID,JOB_LOG_ID,PROCESS_ID,COMP_TYPE FROM t_etl_dataflow_logs WHERE ID=%s AND STATUS=1 LIMIT 1"
    LOGGER.info(sql % ID)
    res = MYSQL.query(sql, (ID,))
    LOGGER.info("res :%s" % res)
    if len(res) > 0:
        return res[0]
    else:
        return None


def update_dataflow_logs_0(ID):
    """修改手工执行节点日志"""
    """status 状态：1 正在执行 2 执行完成 3 执行失败 4 手动终止"""
    sql = "UPDATE t_etl_dataflow_logs SET STATUS=4,END_TIME=NOW(),COST_TIME=UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(START_TIME) WHERE ID=%s AND STATUS=1"
    LOGGER.info(sql % ID)
    MYSQL.execute_sql(sql, (ID,))


def get_proc_logs(DETAIL_LOG_ID):
    """获取执行程序日志"""
    #  AND a.RUN_TYPE=%s
    sql = """
    SELECT a.ID,a.DETAIL_LOG_ID,a.PROCESS_ID,a.PROC_NAME,a.SPARK_NAME,b.COMP_TYPE
    FROM t_etl_proc_log a
    LEFT JOIN t_etl_dataflow_logs b ON a.DETAIL_LOG_ID=b.ID
    WHERE a.DETAIL_LOG_ID IN (%s) AND a.STATUS=1
    """
    LOGGER.info(sql % DETAIL_LOG_ID)
    res = MYSQL.query(sql, (DETAIL_LOG_ID,))
    LOGGER.info("res :%s" % res)
    if len(res) > 0:
        return res
    else:
        return None


def update_proc_logs(DETAIL_LOG_ID, STATUS):
    """修改执行程序日志"""
    """status 状态：1 正在执行 2 执行完成 3 执行失败 4 手动终止 5 正在终止 6 超时终止"""
    if TYPE == "0":
        sql = "UPDATE t_etl_proc_log SET STATUS=%s WHERE DETAIL_LOG_ID IN (%s)"
        LOGGER.info(sql % (STATUS, DETAIL_LOG_ID))
        MYSQL.execute_sql(sql, (STATUS, DETAIL_LOG_ID))
    else:
        #  AND STATUS=1
        sql = "UPDATE t_etl_proc_log SET STATUS=%s,END_TIME=NOW(),COST_TIME=UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(START_TIME) WHERE DETAIL_LOG_ID IN (%s) AND RUN_TYPE=%s"
        LOGGER.info(sql % (STATUS, DETAIL_LOG_ID, TYPE))
        MYSQL.execute_sql(sql, (STATUS, DETAIL_LOG_ID, TYPE))


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


def main():
    def succ(pid, returncode, outs, errs):
        # 执行成功记录日志
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)

    def fail(pid, returncode, outs, errs):
        # 执行失败记录日志
        LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)

    def ssh_succ(returncode, outs, errs):
        # 执行成功记录日志
        LOGGER.info("exec_cmd returncode:%s" % returncode)
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)

    def ssh_fail(returncode, outs, errs):
        # 执行失败记录日志
        LOGGER.info("exec_cmd returncode:%s" % returncode)
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)

    LOGGER.info("start LSH: %s ,TYPE: %s" % (LSH, TYPE))
    try:
        cmd = "kill -9 %s"
        if TYPE == "1":
            """调度执行"""
            dataflow_logs = get_dataflow_logs_1(LSH)
            if dataflow_logs:
                detail_log_ids = []
                for log in dataflow_logs:
                    detail_log_ids.append(str(log["ID"]))
                proc_logs = get_proc_logs(''.join(detail_log_ids))
                update_proc_logs(''.join(detail_log_ids), 5)
                if proc_logs:
                    for log in proc_logs:
                        if log["COMP_TYPE"] != 5 and log["COMP_TYPE"] != "5":
                            tmp_cmd = cmd % log["PROCESS_ID"]
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            ProcUtil().single_pro(tmp_cmd, succ, fail)
                        if log["COMP_TYPE"] == 3 or log["COMP_TYPE"] == "3":
                            tmp_cmd = """
                            pid=$(%s application -list|grep %s| awk '{print $1}')
                            if [ ! -n "$pid" ]; then
                            echo "not"
                            else
                            %s application -kill ${pid}
                            fi
                            """ % (CONF.get("conf", "yarn_bin"), log["SPARK_NAME"], CONF.get("conf", "yarn_bin"))
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            ProcUtil().single_pro(tmp_cmd, succ, fail)
                        elif log["COMP_TYPE"] == 5 or log["COMP_TYPE"] == "5":
                            tmp_cmd = "%s %s" % (CONF.get("conf", "kill_kettle_sh"), log["PROC_NAME"])
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            hostnode = get_kettle_hostnode()
                            ProcUtil().ssh_exec_cmd(
                                hostnode["HOST_IP"],
                                hostnode["HOST_PORT"],
                                hostnode["USER_NAME"],
                                des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(hostnode["PASSWORD"])),
                                tmp_cmd,
                                ssh_succ,
                                ssh_fail
                            )
                update_proc_logs(''.join(detail_log_ids), 4)
            update_dataflow_logs_1(LSH)
        elif TYPE == "2":
            """手工执行"""
            dataflow_log = get_dataflow_logs_2(LSH)
            if dataflow_log:
                proc_logs = get_proc_logs(dataflow_log["ID"])
                update_proc_logs(dataflow_log["ID"], 5)
                if proc_logs:
                    for log in proc_logs:
                        if log["COMP_TYPE"] != 5 and log["COMP_TYPE"] != "5":
                            tmp_cmd = cmd % log["PROCESS_ID"]
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            ProcUtil().single_pro(tmp_cmd, succ, fail)
                        if log["COMP_TYPE"] == 3 or log["COMP_TYPE"] == "3":
                            tmp_cmd = """
                            pid=$(%s application -list|grep %s| awk '{print $1}')
                            if [ ! -n "$pid" ]; then
                            echo "not"
                            else
                            %s application -kill ${pid}
                            fi
                            """ % (CONF.get("conf", "yarn_bin"), log["SPARK_NAME"], CONF.get("conf", "yarn_bin"))
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            ProcUtil().single_pro(tmp_cmd, succ, fail)
                        elif log["COMP_TYPE"] == 5 or log["COMP_TYPE"] == "5":
                            tmp_cmd = "%s %s" % (CONF.get("conf", "kill_kettle_sh"), log["PROC_NAME"])
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            hostnode = get_kettle_hostnode()
                            ProcUtil().ssh_exec_cmd(
                                hostnode["HOST_IP"],
                                hostnode["HOST_PORT"],
                                hostnode["USER_NAME"],
                                des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(hostnode["PASSWORD"])),
                                tmp_cmd,
                                ssh_succ,
                                ssh_fail
                            )
                update_proc_logs(dataflow_log["ID"], 4)
            update_dataflow_logs_2(LSH)
            pass
        elif TYPE == "0":
            """超时终止程序"""
            dataflow_log = get_dataflow_logs_0(LSH)
            if dataflow_log:
                proc_logs = get_proc_logs(dataflow_log["ID"])
                update_proc_logs(dataflow_log["ID"], 5)
                if proc_logs:
                    for log in proc_logs:
                        if log["COMP_TYPE"] != 5 and log["COMP_TYPE"] != "5":
                            tmp_cmd = cmd % log["PROCESS_ID"]
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            ProcUtil().single_pro(tmp_cmd, succ, fail)
                        if log["COMP_TYPE"] == 3 or log["COMP_TYPE"] == "3":
                            tmp_cmd = """
                            pid=$(%s application -list|grep %s| awk '{print $1}')
                            if [ ! -n "$pid" ]; then
                            echo "not"
                            else
                            %s application -kill ${pid}
                            fi
                            """ % (CONF.get("conf", "yarn_bin"), log["SPARK_NAME"], CONF.get("conf", "yarn_bin"))
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            ProcUtil().single_pro(tmp_cmd, succ, fail)
                        elif log["COMP_TYPE"] == 5 or log["COMP_TYPE"] == "5":
                            tmp_cmd = "%s %s" % (CONF.get("conf", "kill_kettle_sh"), log["PROC_NAME"])
                            LOGGER.info("cmd: %s" % tmp_cmd)
                            hostnode = get_kettle_hostnode()
                            ProcUtil().ssh_exec_cmd(
                                hostnode["HOST_IP"],
                                hostnode["HOST_PORT"],
                                hostnode["USER_NAME"],
                                des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(hostnode["PASSWORD"])),
                                tmp_cmd,
                                ssh_succ,
                                ssh_fail
                            )
                update_proc_logs(dataflow_log["ID"], 6)
            pass
    except Exception as e:
        LOGGER.info("error: %s" % e)

    LOGGER.info("end LSH: %s ,TYPE: %s" % (LSH, TYPE))


if __name__ == '__main__':
    read_config()
    init_param()
    main()

