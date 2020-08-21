#!/usr/bin/python
# coding=utf-8
"""
@name 流程执行器
@author jiangbing
@version 1.0.0
@update_time 2018-06-25
@comment 20180625 V1.0.0  jiangbing 新建
"""
import sys
import getopt
import requests
import time
import json
import configparser
from utils.DataflowXmlUtil import DataflowXmlUtil
from utils.ProcUtil import ProcUtil
from utils.DBUtil import MysqlUtil
from utils.LogUtil import Logger
import utils.FuncUtil as fu
import datetime
from pyDes import des, PAD_PKCS5
import base64
import time
import threading

# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/RunDataflow.conf"
CONF = None
MYSQL = None
# 调度类型
TYPE = 1
# 数据流程ID
DATAFLOWID = None
# 数据日期
RQ = None
# 作业批次ID
BATCHID = None
# 扩展参数
VAR = {}
# 宏
MACRO = None
# 日志
LOGGER = None
LOG_FILE = None


def show_help():
    """指令帮助"""
    print("""
    -d    数据流程ID
    -r    数据日期
    -b    作业批次ID
    """)
    sys.exit()


def validate_input():
    """验证参数"""
    if DATAFLOWID is None:
        print("please input -d")
        LOGGER.info("please input -d")
        sys.exit(1)
    if RQ is None:
        print("please input -r")
        LOGGER.info("please input -r")
        sys.exit(1)
    if BATCHID is None:
        print("please input -b")
        LOGGER.info("please input -b")
        sys.exit(1)


def init_param():
    """初始化参数"""
    # -d 2 -r 20180101 -b 1 -v KSRQ=20180101&JSRQ=20180102
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:d:r:j:b:v:", ["help", "type=", "dataflowid=", "rq=", "jobid=", "batchid=", "var="])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        show_help()
        sys.exit(1)

    for name, value in opts:
        if name in ("-h", "--help"):
            show_help()
        if name in ("-d", "--dataflowid"):
            global DATAFLOWID
            DATAFLOWID = value
        if name in ("-r", "--rq"):
            global RQ
            RQ = value
        if name in ("-b", "--batchid"):
            global BATCHID
            BATCHID = value
        if name in ("-v", "--var"):
            if value != "":
                global VAR
                tmp = value.split("&")
                for item in tmp:
                    t = item.split("=")
                    VAR[t[0]] = t[1]
    validate_input()


def finish_callback(costTime, endTime):
    """
    执行完成回调
    :param status 状态：1 正在执行 2 执行完成 3 执行失败 4 手动终止
    """
    status = 2
    sql = "select * from t_etl_dataflow_logs where JOB_LOG_ID=%s AND RUN_TYPE=%s and STATUS=3"
    res = MYSQL.query(sql, (BATCHID, TYPE))
    if len(res) > 0:
        status = 3
    sql = "select * from t_etl_dataflow_logs where JOB_LOG_ID=%s AND RUN_TYPE=%s and STATUS=4"
    res = MYSQL.query(sql, (BATCHID, TYPE))
    if len(res) > 0:
        status = 4
    param = {"jobLogId": int(BATCHID), "costTime": costTime, "endTime": endTime * 1000, "status": status}
    LOGGER.info("callback send: %s" % param)
    response = requests.post(CONF.get("conf", "finish_callback_url"), data=json.dumps(param), headers={'content-type': "application/json"})
    LOGGER.info("callback receive: %s" % response.text)


def deal_node_param(nodeParam):
    """解析宏定义参数"""
    param = ""
    for key in nodeParam:
        # LOGGER.info("nodeParam[key]: %s" % nodeParam[key])
        if str(nodeParam[key]).startswith("@"):
            # LOGGER.info("MACRO: %s,MACRO[nodeParam[key]]:%s" % (MACRO, MACRO[nodeParam[key]]))
            if MACRO is not None and MACRO[nodeParam[key]] is not None:
                v = nodeParam[key]
                tmp = MACRO[nodeParam[key]].split(";")
                if tmp[0] == "-r":
                    v = RQ
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
    if param.find("RQ") < 0 and param.find("rq") < 0:
        param += "RQ=%s" % RQ
    elif param != "":
        param = param[:-1]
    LOGGER.info("xml nodeParam: %s" % param)
    return param


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


def get_dataflow(id):
    """根据id获取流程"""
    sql = "select * from t_etl_dataflow where ID=%s"
    LOGGER.info(sql % id)
    res = MYSQL.query(sql, (id,))
    LOGGER.info("get_dataflow: %s" % res)
    if len(res) > 0:
        return res[0]
    else:
        return None


def get_datahandlers(id):
    """根据id获取数据处理程序"""
    sql = """
    SELECT a.ID,a.`NAME`,a.TYPE,a.PATH,a.DESCRIPTION,a.HOST_ID,b.HOST_IP,b.HOST_PORT,b.USER_NAME,b.`PASSWORD` FROM t_job_datahandlers a
LEFT JOIN t_srm_hostnode b ON a.HOST_ID=b.ID
WHERE a.ID=%s LIMIT 1
"""
    LOGGER.info(sql % id)
    res = MYSQL.query(sql, (id,))
    if len(res) > 0:
        return res[0]
    else:
        return None


def get_sqoop_proc(id):
    """根据id获取Sqoop处理程序"""
    sql = """
SELECT a.ID,a.`NAME`,a.DESCRIPTION,a.OPR_TYPE
FROM t_job_sqoop_group a
WHERE a.ID=%s LIMIT 1
"""
    LOGGER.info(sql % id)
    res = MYSQL.query(sql, (id,))
    sql = """
SELECT a.ID
FROM t_srm_hostnode a
WHERE a.TYPE=1 AND a.ISDEFAULT=1 LIMIT 1
"""
    LOGGER.info(sql)
    res1 = MYSQL.query(sql, ())
    if len(res) > 0:
        res[0]["HOST_ID"] = res1[0]["ID"]
        return res[0]
    else:
        return None


def get_kettle_proc(id):
    """根据id获取kettle处理程序"""
    sql = """
SELECT a.ID,a.`NAME`,a.TYPE,a.KETTLE_ID,a.DESCRIPTION,a.HOST_ID,b.HOST_IP,b.HOST_PORT,b.USER_NAME,b.`PASSWORD`
FROM t_job_kettle a
LEFT JOIN t_srm_hostnode b ON a.HOST_ID=b.ID
WHERE a.ID=%s LIMIT 1
    """
    LOGGER.info(sql % id)
    res = MYSQL.query(sql, (id,))
    if len(res) > 0:
        path_sql = "select getParentList(a.ID_DIRECTORY) as path, a.NAME from r_job a where ID_JOB=%s"
        LOGGER.info(path_sql % res[0]["KETTLE_ID"])
        path_res = MYSQL.db_query(
            DB_CONF.get("kettle_db", "host"),
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get("kettle_db", "user"))),
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get("kettle_db", "password"))),
            DB_CONF.get("kettle_db", "database"),
            DB_CONF.get("kettle_db", "port"),
            path_sql,
            (res[0]["KETTLE_ID"],)
        )
        res[0]["PATH"] = path_res[0]["path"]
        res[0]["JOB_NAME"] = path_res[0]["NAME"]
        return res[0]
    else:
        return None


def get_ods_proc(id):
    """根据id获取数据处理程序"""
    sql = """
    SELECT a.ID,a.SRC_DB_ID,a.SRC_USER,a.HOST_ID,b.HOST_IP,b.HOST_PORT,b.USER_NAME,b.`PASSWORD`,c.SOURCE_NAME
FROM t_ods_group_relation a
LEFT JOIN t_srm_hostnode b ON a.HOST_ID=b.ID
LEFT JOIN t_srm_datasource c ON a.SRC_DB_ID=c.ID
WHERE a.ID=%s LIMIT 1
"""
    LOGGER.info(sql % id)
    res = MYSQL.query(sql, (id,))
    if len(res) > 0:
        return res[0]
    else:
        return None


def start_dataflow_logs(COMP_ID, COMP_NAME, COMP_TYPE, HOST_ID, KETTLE_BATCHID = None):
    """记录节点日志"""
    sql = """
    insert into t_etl_dataflow_logs(
    `JOB_LOG_ID`,
    `COMP_ID`,
    `COMP_NAME`,
    `COMP_TYPE`,
    `STAT_DATE`,
    `STATUS`,
    `START_TIME`,
    `RUN_TYPE`,
    `HOST_ID`,
    `KETTLE_BATCHID`
    )values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    LOGGER.info(sql % (BATCHID, COMP_ID, COMP_NAME, COMP_TYPE, RQ, 1, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), TYPE, HOST_ID, KETTLE_BATCHID))
    id = MYSQL.execute_sql(sql, (BATCHID, COMP_ID, COMP_NAME, COMP_TYPE, RQ, 1, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), TYPE, HOST_ID, KETTLE_BATCHID))
    return id


def end_dataflow_logs(ID, COST_TIME, STATUS):
    """修改节点日志"""
    sql = "UPDATE t_etl_dataflow_logs SET END_TIME=%s,COST_TIME=%s WHERE ID=%s"
    LOGGER.info(sql % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, ID))
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, ID))
    sql = "UPDATE t_etl_dataflow_logs SET STATUS=%s WHERE ID=%s AND STATUS=1"
    LOGGER.info(sql % (STATUS, ID))
    MYSQL.execute_sql(sql, (STATUS, ID))


def update_dataflow_logs(ID, PROCESS_ID):
    """修改节点日志"""
    sql = "UPDATE t_etl_dataflow_logs SET PROCESS_ID=%s WHERE ID=%s"
    LOGGER.info(sql % (PROCESS_ID, ID))
    MYSQL.execute_sql(sql, (PROCESS_ID, ID))


def restart_dataflow_logs(ID):
    """修改节点日志"""
    sql = "UPDATE t_etl_dataflow_logs SET END_TIME=NULL, COST_TIME=NULL, STATUS=1 WHERE ID=%s"
    LOGGER.info(sql % ID)
    MYSQL.execute_sql(sql, (ID,))


def is_stop(exception):
    """是否终止执行程序"""
    sql = "SELECT ID FROM t_etl_dataflow_logs WHERE JOB_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=4"
    LOGGER.info(sql % (BATCHID, TYPE))
    res = MYSQL.query(sql, (BATCHID, TYPE))
    if len(res) > 0:
        return True

    if exception == "1":
        sql = "SELECT ID FROM t_etl_dataflow_logs WHERE JOB_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=3"
        LOGGER.info(sql % (BATCHID, TYPE))
        res = MYSQL.query(sql, (BATCHID, TYPE))
        return len(res) > 0

    return False


def exec_dataflow(str_xml):
    """执行流程"""
    LOGGER.info("start dataflow")
    start = int(time.time())
    try:
        DataflowXmlUtil(str_xml, CONF.get("conf", "allow_parallel"), CONF.get("conf", "parallel_num")).run(exec_dataflow_node)
        end = int(time.time())
    except Exception as e:
        LOGGER.info("error: %s" % e)
        end = int(time.time())
    try:
        finish_callback(end - start, end)
    except Exception as e:
        LOGGER.info("error: %s" % e)
    LOGGER.info("end dataflow")


def exec_dataflow_node(dataflow, node):
    """执行流程节点"""

    """nodeType 节点类型：
0：端节点（开始结束）
1：Impala节点
2：hive节点
3：Spark节点
4：Spoop节点
5：Kettle节点
6：其他节点
7：sh节点
9：inceptor节点
10：ods节点
"""
    nodeType = dataflow.get_sub_node_data(node, "nodeType")
    exception = dataflow.get_sub_node_data(node, "exception")
    if is_stop(exception) is True:
        return
    start = int(time.time())
    LOGGER.info("start: %s" % dataflow.get_attr(node, "name"))
    if nodeType == "1":
        datahandlers = get_datahandlers(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(dataflow.get_sub_node_data(node, "sourceId"), dataflow.get_attr(node, "name"), dataflow.get_sub_node_data(node, "nodeType"), datahandlers["HOST_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            var_param = deal_node_param(nodeParam)
            # -t 1 -l 174 --host_id 2 -v RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/impala/conf/tran_jzjysj.json
            param = "-t %s -l %s --file=\"%s\" -v \"%s\"" % (TYPE, id, datahandlers["PATH"], var_param)
            cmd = "%s %s/RunImpala.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    elif nodeType == "2":
        datahandlers = get_datahandlers(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(dataflow.get_sub_node_data(node, "sourceId"), dataflow.get_attr(node, "name"), dataflow.get_sub_node_data(node, "nodeType"), datahandlers["HOST_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            var_param = deal_node_param(nodeParam)
            # -t 1 -l 190 -v KSRQ=20180101&JSRQ=20180101&RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/hive/conf/load_apex_cif3.json
            param = "-t %s -l %s --file=\"%s\" -v \"%s\"" % (TYPE, id, datahandlers["PATH"], var_param)
            cmd = "%s %s/RunHive.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    elif nodeType == "3":
        timeout_minute = dataflow.get_sub_node_data(node, "timeout_minute")
        datahandlers = get_datahandlers(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(dataflow.get_sub_node_data(node, "sourceId"), dataflow.get_attr(node, "name"), dataflow.get_sub_node_data(node, "nodeType"), datahandlers["HOST_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        def add_log(pid, returncode, outs, errs):
            LOGGER.info("exec_cmd id:%s,  pid:%s, returncode:%s" % (id, pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)

        def stop_timeout(minute):
            """终止超时程序"""
            time.sleep(int(minute) * 60)
            cmd = "%s %s/RunDataKiller.py -t 0 -l %s" % (CONF.get("conf", "python_bin"), sys.path[0], id)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, add_log, add_log)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            var_param = deal_node_param(nodeParam)
            # -t 1 -l 192 -v KSRQ=20180101&JSRQ=20180101 --file=/home/bigdata/BDWorkflow/comp/spark/conf/spark_jzjyrzqs.json
            param = "-t %s -l %s --file=\"%s\" -v \"%s\"" % (TYPE, id, datahandlers["PATH"], var_param)
            if timeout_minute != "" and timeout_minute != "0" and timeout_minute != 0:
                threading.Thread(target=stop_timeout, args=(timeout_minute,), daemon=True).start()
            cmd = "%s %s/RunSpark.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
            cost = int(time.time()) - start
            if timeout_minute != "" and timeout_minute != "0" and timeout_minute != 0 and cost > int(timeout_minute) * 60:
                time.sleep(3)
                restart_dataflow_logs(id)
                cmd = "%s %s/RunSpark.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
                LOGGER.info("cmd: %s" % cmd)
                ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    elif nodeType == "4":
        proc = get_sqoop_proc(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(proc["ID"], proc["NAME"], 4, proc["HOST_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            var_param = deal_node_param(nodeParam)
            param = "-t %s -l %s -v \"%s\" --opr_type=%s --group_id=%s" % (TYPE, id, var_param, proc["OPR_TYPE"], proc["ID"])
            cmd = "%s %s/RunSqoop.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    elif nodeType == "5":
        proc = get_kettle_proc(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(proc["ID"], proc["NAME"], proc["TYPE"], proc["HOST_ID"], proc["KETTLE_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            var_param = deal_node_param(nodeParam)
            # -t 1 -l 195 -v RQ=20180101 --job=ABOSS数据采集_融资融券_增量作业 --path=/ABOSS/作业管理 --desc=ABOSS数据采集_融资融券_增量作业 --job_name=ABOSS数据采集_融资融券_增量作业
            param = "-t %s -l %s -v \"%s\" --job=\"%s\" --path=\"%s\" --desc=\"%s\" --job_name=\"%s\"" % (
            TYPE, id, var_param, proc["NAME"], proc["PATH"], proc["DESCRIPTION"], proc["JOB_NAME"])
            cmd = "%s %s/RunKettle.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    elif nodeType == "7":
        datahandlers = get_datahandlers(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(dataflow.get_sub_node_data(node, "sourceId"), dataflow.get_attr(node, "name"), dataflow.get_sub_node_data(node, "nodeType"), datahandlers["HOST_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            var_param = deal_node_param(nodeParam)
            # -t 1 -l 174 -v RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/impala/conf/tran_jzjysj.json
            param = "-t %s -l %s --file=\"%s\" -v \"%s\"" % (TYPE, id, datahandlers["PATH"], var_param)
            cmd = "%s %s/RunSh.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    elif nodeType == "9":
        datahandlers = get_datahandlers(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(dataflow.get_sub_node_data(node, "sourceId"), dataflow.get_attr(node, "name"), dataflow.get_sub_node_data(node, "nodeType"), datahandlers["HOST_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            var_param = deal_node_param(nodeParam)
            # -t 1 -l 190 -v KSRQ=20180101&JSRQ=20180101&RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/hive/conf/load_apex_cif3.json
            param = "-t %s -l %s --file=\"%s\" -v \"%s\"" % (TYPE, id, datahandlers["PATH"], var_param)
            cmd = "%s %s/RunInceptor.pyc %s" % (CONF.get("conf", "python_bin"), sys.path[0], param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    elif nodeType == "10":
        datahandlers = get_ods_proc(dataflow.get_sub_node_data(node, "sourceId"))
        # 开始记录节点日志
        id = start_dataflow_logs(dataflow.get_sub_node_data(node, "sourceId"), dataflow.get_attr(node, "name"), dataflow.get_sub_node_data(node, "nodeType"), datahandlers["HOST_ID"])

        def succ(pid, returncode, outs, errs):
            # 执行成功记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            end_dataflow_logs(id, int(time.time()) - start, 2)

        def fail(pid, returncode, outs, errs):
            # 执行失败记录节点日志
            LOGGER.info("exec_cmd pid:%s, returncode:%s" % (pid, returncode))
            LOGGER.info("exec_cmd outs: %s" % outs)
            LOGGER.info("exec_cmd errs: %s" % errs)
            if returncode == -9:
                end_dataflow_logs(id, int(time.time()) - start, 4)
            else:
                end_dataflow_logs(id, int(time.time()) - start, 3)

        def before(pid):
            # 进程运行前操作
            update_dataflow_logs(id, pid)

        try:
            nodeParam = json.loads(dataflow.get_sub_node_data(node, "nodeParam"))
            nodeParam["DATAFLOW_LOGS_ID"] = id
            nodeParam["BID"] = dataflow.get_sub_node_data(node, "sourceId")
            nodeParam["SRCDB_ID"] = datahandlers["SRC_DB_ID"]
            nodeParam["DBUSER"] = datahandlers["SRC_USER"]
            nodeParam["SRCDB_NAME"] = datahandlers["SOURCE_NAME"]
            var_param = deal_node_param(nodeParam)
            # -t 1 -l 174 -v RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/impala/conf/tran_jzjysj.json
            # param = "-t %s -l %s --file=\"%s\" -v \"%s\"" % (TYPE, id, datahandlers["PATH"], var_param)
            cmd = "sh %s/cal_RunOds.sh \"%s\"" % (sys.path[0], var_param)
            LOGGER.info("cmd: %s" % cmd)
            ProcUtil().single_pro(cmd, succ, fail, before)
        except Exception as e:
            LOGGER.info("error: %s" % e)
            end_dataflow_logs(id, int(time.time()) - start, 3)
        pass
    else:
        pass
    LOGGER.info("end: %s" % dataflow.get_attr(node, "name"))


def read_config():
    # 读取配置文件
    global CONF, DB_CONF, LOGGER, LOG_FILE
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH)
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
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
    # 获取流程信息
    dataflow = get_dataflow(DATAFLOWID)
    # 获取宏定义
    init_dataflow_macro()
    # 执行流程
    exec_dataflow(dataflow["DEFINITION"])


if __name__ == '__main__':
    read_config()
    init_param()
    main()
