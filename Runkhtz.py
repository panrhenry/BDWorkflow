#!/usr/bin/python
# coding=utf-8
"""
@name 客户标签执行器
@author zyp
@version 1.0.0
@update_time 2018-06-25
@comment 20180625 V1.0.0  jiangbing 新建
"""
import sys
import getopt
import json
import configparser
from utils.ProcUtil import ProcUtil
from utils.DBUtil import MysqlUtil
from utils.LogUtil import Logger
import datetime
from pyDes import des, PAD_PKCS5
import base64
import calendar
import time

# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/Runkhtz.conf"
DATAFLOW_CONFIG_PATH = sys.path[0] + "/conf/RunDataflow.conf"
CONF = None
MYSQL = None
# 密钥
DES_KEY = ""
# 调度类型
TYPE  = None
# 数据流程ID
DATAFLOWID = None
# 数据日期
RQ = None
# 作业批次ID
BATCHID = None
# 扩展参数
VAR = None
# 宏
MACRO = None
# 日志
LOGGER = None
LOG_FILE = None
# 节点异常终止
SHUTDOWN_FLAG = True
CODE = None
LOGID = None
EXCUTE_ALL=None
OPTLOGID =None


def show_help():
    """指令帮助"""
    print( """
    -c 客户标签编码 多个用,号隔开 非必填
    -t 执行方式 1.自动执行 2.手工执行 默认2
    -l 日志id 不填将生成一条新日志 非必填
    -a 执行全部，忽略执行周期限制
    -h 显示帮助
    -o 标签操作日志id
    """)
    sys.exit()


def validate_input():
    """验证参数"""
    pass
#是否是绝对路径
def isAbsolutelyPath(path=''):
    return path.startswith('/') or path.startswith(':',2)
def getAbsolutelyPath(path=''):
    if isAbsolutelyPath(path):
        return path
    return sys.path[0]+"/"+path

def init_param():
    """初始化参数"""
    # -t 1 -code "AGE"
    global TYPE,CODE,LOGID,EXCUTE_ALL,OPTLOGID
    TYPE = 2
    LOGID = None
    LOGID=None
    EXCUTE_ALL=None
    OPTLOGID = None
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:c:l:t:ao:", ["help","code","t","a","optlogid"])
    except getopt.GetoptError:
        show_help()
        sys.exit(1)
    print(opts)
    for name, value in opts:
        if name in ("-h", "--help"):
            show_help()
        if name in ("-c", "--code"):
            CODE = value
        if name in ("-l","--l"):
            LOGID = value
        if name in ("-t","--t"):
            TYPE = value
        if name in ("-a","--a"):
            EXCUTE_ALL = 1
        if name in ("-o","--o","-optlogid",'--optlogid'):
            OPTLOGID = value
    validate_input()

def query_datahandlers(ids=''):
    """获取标签处理程序"""
    sql = """
    SELECT
	t1.ID,
	t1.`NAME`,
	t1.TYPE,
	t1.PATH,
	t1.DESCRIPTION,
	t1.HOST_ID,
	t2.ID AS LABEL_ID,
	t2.STATUS,
	t2.RUN_JSON_PLAN,
	t2.LAST_RUN_TIME,
	t3.HOST_IP,
	t2.LABEL_CODE,
	t2.LABEL_NAME,
	t3.HOST_PORT,
	t3.USER_NAME,
	t3.`PASSWORD`
FROM
	`t_job_datahandlers` t1
    JOIN t_khtz_label t2
    JOIN t_srm_hostnode t3
where t1.ID = t2.DATAHANDLERS_ID
    AND t1.HOST_ID = t3.ID
    """
    if ids is None or ids.lstrip()=='':
        sql +=" AND t2.STATUS = 1 "
        res = MYSQL.query(sql,())
    else:
        lableIds = ids.split(',')
        sql += " and  t2.ID in ("
        for id in lableIds:
            sql += "%s,"
        sql =sql[0:-1]+")"
        res = MYSQL.query(sql, tuple(lableIds))
    LOGGER.info("query_datahandlers: %s" % res)
    print(len(res))
    if len(res) > 0:
        return res
    else:
        return None

def start_dataflow_logs(COMP_ID, COMP_NAME, COMP_TYPE, HOST_ID, KETTLE_BATCHID = None):
    """记录节点日志"""
    id = MYSQL.query("select bd.nextval('t_etl_dataflow_logs') as id FROM DUAL", ())[0]["id"]
    sql = """
    insert into t_etl_dataflow_logs(
    `ID`,
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
    )values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    MYSQL.execute_sql(sql, (
        id, BATCHID, COMP_ID, COMP_NAME, COMP_TYPE, RQ, 1, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), TYPE, HOST_ID, KETTLE_BATCHID))
    return id


def end_dataflow_logs(ID, COST_TIME, STATUS):
    """修改节点日志"""
    sql = "UPDATE t_etl_dataflow_logs SET END_TIME=%s,COST_TIME=%s WHERE ID=%s"
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, ID))
    sql = "UPDATE t_etl_dataflow_logs SET STATUS=%s WHERE ID=%s AND STATUS=1"
    MYSQL.execute_sql(sql, (STATUS, ID))

    if SHUTDOWN_FLAG is True:
        sql = "select * from t_etl_dataflow_logs where ID=%s"
        res = MYSQL.query(sql, (ID,))
        LOGGER.info("t_etl_dataflow_logs: %s" % res)
        if len(res) > 0 and str(res[0]["STATUS"]) == "3":
            raise RuntimeError("部分标签计算失败")

def read_config():
    # 读取配置文件
    global DB_CONF, CONF, LOGGER, LOG_FILE,DATAFLOW_CONFIG
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH)
    DATAFLOW_CONFIG = configparser.ConfigParser()
    DATAFLOW_CONFIG.read(DATAFLOW_CONFIG_PATH)
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
def exec_Khtz(id,datahandlers):
    """执行流程节点"""

    """类型：
    1：Impala节点
    2：hive节点
    3：Spark节点
    4：Spoop节点
    5：Kettle节点
    6：其他节点
    7：sh节点
    8:sparkSql
    """
    nodeType = datahandlers["TYPE"]
    exception = 0
    global SHUTDOWN_FLAG
    SHUTDOWN_FLAG = True
    if exception is not None and exception == "0":
        SHUTDOWN_FLAG = False
    elif nodeType == 3:
        LOGGER.info("开始执行：%s" % datahandlers["NAME"])
        var_param ='detaillogid=%s&runtype=%s' % (id,TYPE if datahandlers['STATUS']==1 else  3)
        # ssh远程连接
        # -t 1 -l 192 -v KSRQ=20180101&JSRQ=20180101 --file=/home/bigdata/BDWorkflow/comp/spark/conf/spark_jzjyrzqs.json
        param = "-t %s -l %s --file=%s -v \"%s\"" % (TYPE, id, getAbsolutelyPath(datahandlers["PATH"]), var_param)
        def succ(pid, returncode, out, errs):
            # 执行成功记录节点日志
            LOGGER.info("out: %s" % out)
            LOGGER.info("errs: %s" % errs)
            LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
        def fail(pid, returncode, out, errs):
            # 执行失败记录节点日志
            LOGGER.info("out: %s" % out)
            LOGGER.info("errs: %s" % errs)
            LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
            raise RuntimeError("执行%s出错了" % (datahandlers["NAME"]))

        cmd = "%s %s/RunSpark.pyc %s" % (DATAFLOW_CONFIG.get("conf", "python_bin"), sys.path[0], param)
        LOGGER.info("cmd：%s" % cmd)
        ProcUtil().single_pro(cmd, succ, fail)
        LOGGER.info("结束执行：%s" % datahandlers["NAME"])
        pass
    elif nodeType == 7:
        LOGGER.info("开始执行：%s" % datahandlers["NAME"])
        var_param =""
        # ssh远程连接
        # -t 1 -l 174 -v RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/impala/conf/tran_jzjysj.json
        param = "-t %s -l %s -n %s --file=%s -v \"%s\"" % (TYPE, id, datahandlers["NAME"], getAbsolutelyPath(datahandlers["PATH"]), var_param)
        def succ(pid, returncode, out, errs):
            # 执行成功记录节点日志
            LOGGER.info("out: %s" % out)
            LOGGER.info("errs: %s" % errs)
            LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
        def fail(pid, returncode, out, errs):
            # 执行失败记录节点日志
            LOGGER.info("out: %s" % out)
            LOGGER.info("errs: %s" % errs)
            LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
            raise RuntimeError("执行%s出错了" % (datahandlers["NAME"]))
        cmd = "%s %s/RunSh.pyc %s" % (DATAFLOW_CONFIG.get("conf", "python_bin"), sys.path[0], param)
        LOGGER.info("cmd：%s" % cmd)
        ProcUtil().single_pro(cmd, succ, fail)
        LOGGER.info("结束执行：%s" % datahandlers["NAME"])
        pass
    elif nodeType == 8:
        LOGGER.info("开始执行：%s" % datahandlers["NAME"])
        var_param ="khbqid=%s&detaillogid=%s&runtype=%s" % (datahandlers['LABEL_ID'],id,TYPE if datahandlers['STATUS']==1 else  3)
        # ssh远程连接
        # -t 1 -l 174 -v RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/impala/conf/tran_jzjysj.json
        param = "-t %s -l %s --file=%s -v \"%s\"" % (TYPE, id, getAbsolutelyPath(CONF.get('conf','json_file')), var_param)
        def succ(pid, returncode, out, errs):
            # 执行成功记录节点日志
            LOGGER.info("out: %s" % out)
            LOGGER.info("errs: %s" % errs)
            LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
        def fail(pid, returncode, out, errs):
            # 执行失败记录节点日志
            LOGGER.info("out: %s" % out)
            LOGGER.info("errs: %s" % errs)
            LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
            raise RuntimeError("执行%s出错了" % (datahandlers["NAME"]))
        cmd = "%s %s/RunSpark.pyc %s" % (DATAFLOW_CONFIG.get("conf", "python_bin"), sys.path[0], param)
        print(cmd)
        LOGGER.info("cmd：%s" % cmd)
        ProcUtil().single_pro(cmd, succ, fail)
        LOGGER.info("结束执行：%s" % datahandlers["NAME"])
        pass
    else:
        pass
def pushES(id,pushEsLableCode):
    LOGGER.info("推送es：")
    # 开始记录节点日
    # ssh远程连接
    # -t 1 -l 174 -v RQ=20180101 --file=/home/bigdata/BDWorkflow/comp/impala/conf/tran_jzjysj.json
    var_param ='khbqid=%s&detaillogid=%s&runtype=%s' % (pushEsLableCode,id,TYPE)
    param = "-t %s -l %s --file=%s -v \"%s\"" % (TYPE, id, getAbsolutelyPath(CONF.get('conf','push_es_json_file')),var_param)
    def succ(pid, returncode, out, errs):
        # 执行成功记录节点日志
        LOGGER.info("out: %s" % out)
        LOGGER.info("errs: %s" % errs)
        LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))

    def fail(pid, returncode, out, errs):
        # 执行失败记录节点日志
        LOGGER.info("out: %s" % out)
        LOGGER.info("errs: %s" % errs)
        LOGGER.info("pid: %s,returncode: %s" % (pid, returncode))
        raise RuntimeError("执行推送es出错了")

    cmd = "%s %s/RunSpark.pyc %s" % (DATAFLOW_CONFIG.get("conf", "python_bin"), sys.path[0], param)
    print(cmd)
    LOGGER.info("cmd：%s" % cmd)
    ProcUtil().single_pro(cmd, succ, fail)
    LOGGER.info("结束推送es")
    pass
#是否为月初
def is_Month_start_day():
    return time.strftime("%d", time.localtime()) == '01'
#是否为每月15号
def is_Month_15_day():
    return time.strftime("%d", time.localtime()) == '15'
#今日是否为月末
def is_Month_End_day():
    return time.strftime("%Y-%m-%d", time.localtime()) == get_Month_End_day()
#获取月末日期
def get_Month_End_day():
    day_now = time.localtime()
    wday, monthRange = calendar.monthrange(day_now.tm_year, day_now.tm_mon)  # 得到本月的天数 第一返回为月第一日为星期几（0-6）, 第二返回为此月天数
    day_end = '%d-%02d-%02d' % (day_now.tm_year, day_now.tm_mon, monthRange)
    return day_end
def get_run_dataHandels(labelIds):
    list = query_datahandlers(labelIds)
    excuteList = [];
    tranFerList = []
    if CODE is not None or EXCUTE_ALL is not None :
        excuteList = list
    else:
        #过滤不需要执行的标签
        for item in list:
            lastRunTime = item['LAST_RUN_TIME']
            data = json.loads(item['RUN_JSON_PLAN'])
            #按天执行
            if  lastRunTime is None or (data['execType']==1 and data['execType']==1 and datetime.datetime.now().isoformat()>=(lastRunTime+datetime.timedelta(days=data['interval'])).date().isoformat() ):
                excuteList.append(item)
            #按周执行
            elif (data['execType']==2) and (1 if datetime.datetime.now().weekday()==6 else datetime.datetime.now().weekday()+2)  in data['execPeriod']:
                excuteList.append(item)
            #按月执行
            elif  (data['execType']==3):
                if data['monthType']==1 and is_Month_End_day():
                    excuteList.append(item)
                elif data['monthType']==2 and is_Month_15_day():
                     excuteList.append(item)
                elif data['monthType']==3 and is_Month_start_day():
                     excuteList.append(item)
    sqlItem = None;
    #转换执行列表将 sparksql类型的统一起来
    if excuteList is not None:
        for item in excuteList:
            if item['TYPE']==8:
                if sqlItem is None:
                    sqlItem = item
                    sqlItem['LABEL_ID'] = str(sqlItem['LABEL_ID'])
                else:
                    sqlItem['LABEL_ID'] += ","+str(item['LABEL_ID'])
            else:
                tranFerList.append(item)
        if sqlItem is not None:
            tranFerList.append(sqlItem)
    return tranFerList
def execCal():
    # LOGID 上级组件传入的日志id
    # logid 系统应用的logid
    run_dataHandels  = get_run_dataHandels(CODE)
    print(run_dataHandels)
     #
    if len(run_dataHandels)<1:
       LOGGER.info("无标签需要运行。。。。。。。。")
    if LOGID is None:
        logid = start_dataflow_logs(None,'客户标签计算与es推送','3' , 0 if len(run_dataHandels)==0 else run_dataHandels[0]["HOST_ID"])
    else:
        logid = LOGID
    pushEsCode = ''
    start = int(time.time())
    run_result = 1
    for handerls in run_dataHandels:
        pushEsCode += str(handerls['LABEL_ID'])+','
        try:
            exec_Khtz(logid,handerls)
        except RuntimeError:
            LOGGER.info(RuntimeError)
            run_result = 0
    #推送es 启用标签推送es否则不推送
    if len(pushEsCode)>0 and run_dataHandels[0]['STATUS']==1:
        pushEsCode = pushEsCode[0:-1]
        try:
           pushES(logid,pushEsCode)
        except RuntimeError:
            LOGGER.info(RuntimeError)
            run_result = 0
    #标签运行操作日志回调
    if OPTLOGID is not None:
        sql = "UPDATE t_khtz_label_log SET END_TIME=%s,RUN_TIME=%s,STATE=%s WHERE ID=%s"
        MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(time.time()) - start,3 if run_result==1 else 2, OPTLOGID))
    #run_result 判断执行过程有无异常 1 无异常 0含有异常
    if LOGID is None and run_result==1:
        end_dataflow_logs(logid, int(time.time()) - start, 2)
    elif LOGID is None and run_result==0:
         end_dataflow_logs(logid, int(time.time()) - start, 3)
    elif run_result==0:
        raise RuntimeError("部分标签计算失败")
def main():
    #z执行标签计算并推送到ES
    execCal()
if __name__ == '__main__':
    read_config()
    init_param()
    main()
