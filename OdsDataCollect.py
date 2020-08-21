#!/usr/bin/python
# coding=utf-8
"""
@name ODS源系统元数据采集器
@author css
@version 1.0.0
@update_time 2018-10-31
@comment 20191031 V1.0.0  chenshuangshui 新建
"""
import sys
import os
import getopt
import datetime
import time
import configparser
from utils.DBUtil import MysqlUtil
from utils.OracleUtil import OracleUtil
from utils.OdsProcUtil import ProcUtil
from utils.OdsDBUtil import SqlserverUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64
import cx_Oracle
from utils.OSUtil import OSUtil
import multiprocessing

# import threading

# reload(sys)
# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/Ods.conf"
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
# 数据源ID
SRCDB_ID = None
# 重新采集标志
RECOLECT = "false"
# 日志
LOGGER = None
LOG_FILE = None
# 主机
HOSTNODE = None
# 平台类型是否为TDH
IS_TDH = False
# 判断程序是否有出现异常
IS_EXCEPT = 0
# 控制程序多进程并发度
PROC_PARALLELISM = 2
# 数据流程日志id
DATAFLOW_LOGS_ID = "-1"
# 采集分组ID
BID = 0
# 指令帮助
show_helps = """
    --SRCDB_ID            数据源（ALL表示全部）
    --DBUSER              数据库用户("ALL"表示全部）
    --TBLNAME             数据表（"ALL"表示全部）
    --RECOLECT            数据重新采集标志
    --EXEC_KSRQ           运行日期开始（如20181113）
    --EXEC_JSRQ           运行结束日期（如20181113)
    --DATAFLOW_LOGS_ID    数据流程日志id
    --EXCEPTION           异常是否继续执行（"continue"/"break")
        """


def show_help():
    """指令帮助"""
    print(show_helps)
    sys.exit()


def validate_input():
    """验证参数"""
    if SRCDB_ID is None:
        print("please input --SRCDB_ID")
        LOGGER.info("please input --SRCDB_ID")
        sys.exit(1)
    if DBUSER is None:
        print("please input --DBUSER")
        LOGGER.info("please input --DBUSER")
        sys.exit(1)
    if TBLNAME is None:
        print("please input --TBLNAME")
        LOGGER.info("please input --TBLNAME")
        sys.exit(1)
    if EXEC_JSRQ is None:
        print("please input --EXEC_JSRQ")
        LOGGER.info("please input --EXEC_JSRQ")
        sys.exit(1)
    # if PROC_PARALLELISM is None:
    #     print("please input -PROC_PARALLELISM")
    #     LOGGER.info("please input --PROC_PARALLELISM")
    if DATAFLOW_LOGS_ID is None:
        print("please input -DATAFLOW_LOGS_ID")
        LOGGER.info("please input --DATAFLOW_LOGS_ID")
    if BID is None:
        print("please input -BID")
        LOGGER.info("please input --BID")


def init_param():
    """初始化参数"""
    # python OdsGenCreateTblSql.py --SRCDB_ID=33 --DBUSER=ALL --TBLNAME=ALL --EXEC_JSRQ=20181113
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:",
                                   ["help", "SRCDB_ID=", "DBUSER=", "TBLNAME=", "EXEC_KSRQ=", "EXEC_JSRQ=",
                                    "EXCEPTION=",
                                    "RECOLECT=", "DATAFLOW_LOGS_ID=", "BID="])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        global IS_EXCEPT
        IS_EXCEPT = 1
        LOGGER.error("init_param IS_EXCEPT--->" + str(IS_EXCEPT))
        show_help()
        LOGGER.info(show_helps)
        sys.exit(1)

    for name, value in opts:
        if name in ("-h", "--help"):
            show_help()
        if name in ("--SRCDB_ID",):
            global SRCDB_ID
            SRCDB_ID = value
        if name in ("--DBUSER",):
            global DBUSER
            DBUSER = value
        if name in ("--TBLNAME",):
            global TBLNAME
            TBLNAME = value
        if name in ("--EXEC_JSRQ",):
            global EXEC_JSRQ
            EXEC_JSRQ = value
        if name in ("--EXEC_KSRQ",):
            global EXEC_KSRQ
            EXEC_KSRQ = value
        if name in ("--EXCEPTION",):
            global EXCEPTION
            EXCEPTION = value if value is not None else 'continue'
        if name in ("--RECOLECT",):
            global RECOLECT
            RECOLECT = value if value is not None else 'false'
        # if name in ("--PROC_PARALLELISM",):
        #     global PROC_PARALLELISM
        #     PROC_PARALLELISM = value
        if name in ("--DATAFLOW_LOGS_ID",):
            global DATAFLOW_LOGS_ID
            DATAFLOW_LOGS_ID = value
        if name in ("--BID",):
            global BID
            BID = value

    validate_input()


def read_config():
    # 读取配置文件
    global DB_CONF, CONF, LOGGER, LOG_FILE, CREATETBL_DIR, STORAGE_TYPE, HDFS_LOCATION, IS_TDH, PROC_PARALLELISM
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    # 读取配置文件
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH, 'utf8')
    LOG_FILE = (sys.path[0] + "/" + CONF.get("conf", "log_file")) % time.strftime("%Y%m%d", time.localtime())
    LOGGER = Logger(LOG_FILE).logger
    CREATETBL_DIR = (sys.path[0] + CONF.get("conf", "createtbl_dir"))
    STORAGE_TYPE = CONF.get("conf", "storage_type")
    HDFS_LOCATION = CONF.get("conf", "hdfs_location")
    PROC_PARALLELISM = CONF.get("conf", "parallel_num")
    if CONF.get('conf', 'platform') == 'tdh':
        IS_TDH = True

    print(CREATETBL_DIR)
    # 连接数据库
    global MYSQL
    MYSQL = MysqlUtil(
        DB_CONF.get("db", "host"),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get("db", "user"))),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
            base64.b64decode(DB_CONF.get("db", "password"))),
        DB_CONF.get("db", "database"),
        DB_CONF.get("db", "port")
    )
    # 初始化group_concat长度大小
    MYSQL.execute_sql("set group_concat_max_len =2048000", {})


def start_ods_proc_logs(tbl, bid):
    """记录程序日志"""
    try:
        sql = """
        insert into t_ods_srcdb_collect_monitor(
        `SRC_DB`,
        `DB_USER`,
        `TABLE_NAME`,
        `ODS_DB_NAME`,
        `ODS_TBL_NAME`,
        `RUN_STATUS`,
        `START_TIME`,
        `START_RUN_TIME`,
        `COMMENT`,
        `END_RUN_TIME`,
        `DATAFLOW_LOGS_ID`,
        `RELATION_ID`,
        `FTYPE`
        )values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        if RECOLECT == 'false':
            sCOMMENT = ""
        else:
            sCOMMENT = "重新采集"

        if EXEC_KSRQ != EXEC_JSRQ:
            sCOMMENT += "补采历史：开始日期：" + EXEC_KSRQ + " 结束日期：" + EXEC_JSRQ;

        LOGGER.info(sql % (
            tbl["SRC_DB"], tbl["DB_USER"], tbl["TABLE_NAME"], tbl["ODS_DB_NAME"], tbl["ODS_TBL_NAME"], 1,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            EXEC_KSRQ,
            sCOMMENT, EXEC_JSRQ, DATAFLOW_LOGS_ID, bid, '1'))
        # id = MYSQL.execute_sql_sleep(sql, (
        id = MYSQL.execute_sql(sql, (
            tbl["SRC_DB"], tbl["DB_USER"], tbl["TABLE_NAME"], tbl["ODS_DB_NAME"], tbl["ODS_TBL_NAME"], 1,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            EXEC_KSRQ,
            sCOMMENT, EXEC_JSRQ, DATAFLOW_LOGS_ID, bid, '1'))
        LOGGER.info(
            "start_ods_proc_logs table name----->" + tbl["TABLE_NAME"] + "---id-----" + str(
                id))
    except Exception as err:
        LOGGER.error(
            "Error start_ods_proc_logs table name----->" + tbl["TABLE_NAME"] + "---id-----" + str(
                id) + "---Error:" + str(
                err))
        id = 0
    return id


def end_ods_proc_logs(ID, COST_TIME, STATUS):
    """修改程序日志"""
    try:
        sql = """
        UPDATE t_ods_srcdb_collect_monitor 
        SET END_TIME=%s,COST_TIME=%s,RUN_STATUS=%s
        WHERE ID=%s
        """
        MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, STATUS, ID))
    except Exception as err:
        LOGGER.error("end_ods_proc_logs  error--->" + str(err))


def error_ods_proc_logs(ID, COST_TIME, STATUS, SOURCE_CNT, TARGET_CNT):
    """修改程序日志"""
    try:
        sql = """
        UPDATE t_ods_srcdb_collect_monitor 
        SET END_TIME=%s,COST_TIME=%s,RUN_STATUS=%s,SOURCE_COUNT=%s,TARGET_COUNT=%s,STAGE_COUNT=%s
        WHERE ID=%s
        """
        MYSQL.execute_sql(sql, (
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, STATUS, SOURCE_CNT, TARGET_CNT,
            TARGET_CNT,
            ID))
    except Exception as err:
        LOGGER.error("error_ods_proc_logs  error--->" + str(err))


def upd_ods_proc_logs(ID, TAB_CNT_DICT, CMD_STR, RUN_SQL):
    """更新记录数信息"""
    try:
        sql = """
        UPDATE t_ods_srcdb_collect_monitor t,t_ods_srcdb_collect_manage d
           SET t.TABLE_COMMENT=d.TABLE_COMMENT,SOURCE_COUNT=%s,STAGE_COUNT=%s,TARGET_COUNT=%s,SRC_CNT_CMD=%s,STAGE_CNT_CMD=%s,ODS_CNT_CMD=%s,SQOOP_CMD=%s,
           INSERT_CMD=%s
         WHERE t.SRC_DB=d.SRC_DB and t.DB_USER=d.DB_USER and t.TABLE_NAME=d.TABLE_NAME 
           and t.ID=%s AND d.FTYPE='1' AND T.FTYPE='1'
        """

        if TAB_CNT_DICT['SOURCE_COUNT'] == 0:
            TAB_CNT_DICT['TARGET_COUNT'] = 0
            TAB_CNT_DICT['STAGE_COUNT'] = 0

        MYSQL.execute_sql(sql,
                          (TAB_CNT_DICT['SOURCE_COUNT'], TAB_CNT_DICT['STAGE_COUNT'], TAB_CNT_DICT['TARGET_COUNT'],
                           TAB_CNT_DICT['SRC_CNT_CMD'], TAB_CNT_DICT['STAGE_CNT_CMD'], TAB_CNT_DICT['ODS_CNT_CMD'],
                           CMD_STR, RUN_SQL,
                           ID))
        LOGGER.info("upd_ods_proc_logs-------->start")
        LOGGER.info(TAB_CNT_DICT['SOURCE_COUNT'])
        LOGGER.info(TAB_CNT_DICT['TARGET_COUNT'])
        LOGGER.info(TAB_CNT_DICT['STAGE_COUNT'])

        if str(TAB_CNT_DICT['SOURCE_COUNT']).strip() == str(TAB_CNT_DICT['TARGET_COUNT']).strip() \
                and str(TAB_CNT_DICT['SOURCE_COUNT']).strip() == str(TAB_CNT_DICT['STAGE_COUNT']).strip():

            sql = """UPDATE t_ods_srcdb_collect_monitor SET RUN_STATUS=%s WHERE FTYPE='1' AND ID=%s"""
            MYSQL.execute_sql(sql, (2, ID))
            LOGGER.info("status----->2")
        else:
            sql = """UPDATE t_ods_srcdb_collect_monitor SET RUN_STATUS=%s WHERE FTYPE='1' AND ID=%s"""
            MYSQL.execute_sql(sql, (3, ID))
            LOGGER.info("status----->3")
        LOGGER.info("upd_ods_proc_logs-------->end")
    except Exception as err:
        global IS_EXCEPT
        IS_EXCEPT = 1
        LOGGER.error("upd_ods_proc_logs IS_EXCEPT err--->" + str(IS_EXCEPT) + "----->" + str(err))


def get_jyr_list(ksrq, jsrq, dt_mode):
    """根据开始日期和结束日期获取交易日列表"""
    # 根据分区模式不同，获取不同的结果
    if dt_mode == 3:
        length = 4
    elif dt_mode == 2:
        length = 6
    else:
        length = 8
    sql = "SELECT distinct substr(EXC_DATE, 1, %s) as JYR FROM pub_sys.t_jyrjl WHERE substr(EXC_DATE, 1, %s) BETWEEN %s AND %s"
    res = MYSQL.query(sql, (length, length, ksrq, jsrq))
    return res


def get_srcdb_list(id):
    """根据id获取数据源信息"""
    res = None
    if id.isdigit():
        sql = """
            SELECT 
            A.ID,
            A.SOURCE_NAME,
            A.DB_LINK       AS SRC_DB,
            A.HOST_IP,
            A.HOST_PORT,
            A.DATABASE_NAME AS SRC_DATABASE_NAME,
            concat('ods','_',lower(A.SOURCE_NAME)) AS ODS_DB_NAME,
            A.TYPE          AS SRC_TYPE,
            B.NOTE AS DATASORCE_TYPE,
            A.USER_NAME     AS SRC_USER_NAME,
            A.PASSWORD      AS SRC_PASSWORD
             FROM t_srm_datasource A,(SELECT IBM,NOTE FROM bd.txtdm t where t.FLDM='SRM_SJKLX') B
            WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user WHERE FTYPE='1') AND (A.id=%s OR 'ALL'=%s)
            """
    else:
        sql = """
        SELECT 
        A.ID,
        A.SOURCE_NAME,
        A.DB_LINK       AS SRC_DB,
        A.HOST_IP,
        A.HOST_PORT,
        A.DATABASE_NAME AS SRC_DATABASE_NAME,
        concat('ods','_',lower(A.SOURCE_NAME)) AS ODS_DB_NAME,
        A.TYPE          AS SRC_TYPE,
        B.NOTE AS DATASORCE_TYPE,
        A.USER_NAME     AS SRC_USER_NAME,
        A.PASSWORD      AS SRC_PASSWORD
         FROM t_srm_datasource A,(SELECT IBM,NOTE FROM bd.txtdm t where t.FLDM='SRM_SJKLX') B
        WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user WHERE FTYPE='1') AND (A.SOURCE_NAME=%s OR 'ALL'=%s) 
        """
    res = MYSQL.query(sql, (id, id))
    return res


def get_srcdb_user_list(srcdb):
    """根据id获取数据源信息对应的用户"""
    sql = """SELECT DB_USER FROM t_ods_srcdb_user T WHERE T.SRC_DB=%s"""
    res = MYSQL.query(sql, (srcdb,))
    return res


def UPDATE_DATAFLOW_LOGS_PATH(DATAFLOW_LOGS_ID):
    "更新ODS采集日志路径到数据流程表里面"
    try:
        sql = """update t_etl_dataflow_logs set LOG_PATH='%s'  where ID=%s"""
        path = (sys.path[0] + "/" + CONF.get("conf", "log_file")) % time.strftime("%Y%m%d", time.localtime())
        exce_sql = sql % (path, DATAFLOW_LOGS_ID)
        LOGGER.info("UPDATE_DATAFLOW_LOGS_PATH: %s" % exce_sql)
        MYSQL.execute_sql(exce_sql, {})
    except Exception as err:
        LOGGER.error("UPDATE_DATAFLOW_LOGS_PATH  err--->" + str(err))


def update_collect_monitor_except_one(DATAFLOW_LOGS_ID, RELATION_ID, TABLE):
    "更新表里面的执行为运行中的数据"
    try:
        sql = """
        update t_ods_srcdb_collect_monitor set RUN_STATUS=3  where ftype=1 and date(start_time)>='%s' AND DATAFLOW_LOGS_ID=%s AND RELATION_ID=%s AND TABLE_NAME=%s AND RUN_STATUS=1
        """
        exce_sql = sql % (datetime.datetime.now().strftime("%Y-%m-%d"), DATAFLOW_LOGS_ID, RELATION_ID, TABLE)
        LOGGER.info("update_collect_monitor_except_one: %s" % exce_sql)
        MYSQL.execute_sql(exce_sql, {})
    except Exception as e:
        LOGGER.error("update_collect_monitor_except_one error:%s" % str(e))


def update_collect_monitor_except(DATAFLOW_LOGS_ID, RELATION_ID):
    "更新表里面的执行为运行中的数据"
    try:
        sql = """
        update t_ods_srcdb_collect_monitor set RUN_STATUS=3  where ftype=1 and date(start_time)>='%s' AND DATAFLOW_LOGS_ID=%s AND RELATION_ID=%s AND  (RUN_STATUS=3 OR RUN_STATUS=1)
        """
        exce_sql = sql % (datetime.datetime.now().strftime("%Y-%m-%d"), DATAFLOW_LOGS_ID, RELATION_ID)
        LOGGER.info("update_collect_monitor_except: %s" % exce_sql)
        MYSQL.execute_sql(exce_sql, {})
    except Exception as e:
        LOGGER.error("update_collect_monitor_except error:%s" % str(e))


def update_ods_proc_logs(tbl, bid, log_id):
    "异常重新采集"
    try:

        sql = """
        update t_ods_srcdb_collect_monitor set start_time='%s'  where ftype=1 and date(start_time)>='%s'  AND RELATION_ID=%s AND ID=%s
        """
        exce_sql = sql % (
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.datetime.now().strftime("%Y-%m-%d"),
            bid, log_id)
        LOGGER.info("update_ods_proc_logs: %s" % exce_sql)
        MYSQL.execute_sql(exce_sql, {})
    except Exception as e:
        LOGGER.error("update_ods_proc_logs error:%s" % str(e))


def get_collect_monitor_except(DATAFLOW_LOGS_ID, RELATION_ID):
    "获取日志表里面的执行异常的数据"
    res = None
    try:
        sql = """
            select * from t_ods_srcdb_collect_monitor  where ftype=1 and date(start_time)>='%s' AND DATAFLOW_LOGS_ID=%s AND RELATION_ID=%s AND  RUN_STATUS=3
            """
        exce_sql = sql % (datetime.datetime.now().strftime("%Y-%m-%d"), DATAFLOW_LOGS_ID, RELATION_ID)
        print(exce_sql)
        LOGGER.info("get_collect_monitor_except:%s" % exce_sql)
        res = MYSQL.query(exce_sql, {})
        LOGGER.info("get_collect_monitor_except res: %s" % str(res))
    except Exception as e:
        LOGGER.error("get_collect_monitor_except error:%s" % str(e))
    return res


def get_srdb_tbl_list(srcdb, dbuser, tblname):
    """获取对应表的信息"""
    if not tblname[0:1].isalpha():
        dbuser = "'" + dbuser.replace(",", "','") + "'"
        tblname = "'" + tblname.replace(",", "','") + "'"

        sql = """
SELECT A.SRC_DB, C.TYPE, A.DB_USER, A.TABLE_NAME,
       C.TYPE AS SRC_DB_TYPE,
       concat(lower(D.ODS_DB_USER)) AS ODS_DB_NAME,
       concat(lower(A.DB_USER),'_',lower(A.TABLE_NAME)) AS ODS_TBL_NAME,
	   d.COLLECT_STRATEGY,d.INCR_COLNAME,d.INCR_TIMEDIM, d.DT_MODE,d.SPECIAL_FIELD
  FROM t_ods_srcdb_tbl A, t_srm_datasource C,t_ods_srcdb_collect_manage D
 WHERE A.SRC_DB = C.ID
   AND D.STATUS=1
   AND A.SRC_DB=D.SRC_DB 
   AND A.DB_USER=D.DB_USER 
   AND A.TABLE_NAME=D.TABLE_NAME
   AND (A.SRC_DB = %s OR 0=%s)
   AND (A.DB_USER IN (%s) OR 'ALL' IN (%s))
   AND (D.ID IN (%s) OR 'ALL' IN (%s))
   AND A.FTYPE='1' AND D.FTYPE='1'
"""
    else:
        dbuser = "'" + dbuser.replace(",", "','") + "'"
        tblname = "'" + tblname.replace(",", "','") + "'"

        sql = """
    SELECT A.SRC_DB, C.TYPE, A.DB_USER, A.TABLE_NAME,
           C.TYPE AS SRC_DB_TYPE,
           concat(lower(D.ODS_DB_USER)) AS ODS_DB_NAME,
           concat(lower(A.DB_USER),'_',lower(A.TABLE_NAME)) AS ODS_TBL_NAME,
    	   d.COLLECT_STRATEGY,d.INCR_COLNAME,d.INCR_TIMEDIM, d.DT_MODE,d.SPECIAL_FIELD
      FROM t_ods_srcdb_tbl A, t_srm_datasource C,t_ods_srcdb_collect_manage D
     WHERE A.SRC_DB = C.ID
       AND D.STATUS=1
       AND A.SRC_DB=D.SRC_DB 
       AND A.DB_USER=D.DB_USER 
       AND A.TABLE_NAME=D.TABLE_NAME
       AND (A.SRC_DB = %s OR 0=%s)
       AND (A.DB_USER IN (%s) OR 'ALL' IN (%s))
       AND (A.TABLE_NAME IN (%s) OR 'ALL' IN (%s))
       AND A.FTYPE='1' AND D.FTYPE='1'
    """

    exec_sql = sql % (srcdb, srcdb, dbuser, dbuser, tblname, tblname)
    print(exec_sql)
    LOGGER.info("get_srdb_tbl_list sql:%s" % exec_sql)
    res = MYSQL.query(exec_sql, {})
    return res


def get_tab_cols_str(srcdb, dbyser, tbl):
    """获取对应表的字符串信息"""
    try:

        sql = """
        SELECT GROUP_CONCAT(COLUMN_NAME) AS COL_STR FROM (SELECT COLUMN_NAME
        FROM t_ods_srcdb_tblcol t
        WHERE t.SRC_DB = '%s' AND t.DB_USER = '%s' AND t.TABLE_NAME = '%s' AND t.ftype=1 ORDER BY T.COLUMN_ID) D """

        exce_sql = sql % (srcdb, dbyser, tbl)
        LOGGER.info("get_tab_cols_str:%s" % exce_sql)
        res = MYSQL.query(exce_sql, {})
        LOGGER.info("get_tab_cols_str  res :%s" % str(res[0]['COL_STR']))
        return res[0]['COL_STR']
    except Exception as e:
        LOGGER.error("get_tab_cols_str error:%s" % str(e) + "---res[0]['COL_STR']-->" + str(res[0]['COL_STR']))


def get_sqoop_connect(dblink, user, pwd):
    """获取sqoop的链接串"""
    conn_str = ''
    conn_user = user
    conn_pwd = pwd
    conn_user = " --username " + conn_user
    conn_pwd = " --password " + conn_pwd
    conn_str = conn_user + conn_pwd + " --connect " + "\"" + dblink + "\""
    return conn_str


def get_sqoop_all_table(srcdb_type, conn_str, src_db, src_tabname, tgt_db, tgt_tabname, table_special_field):
    """处理全量加载到贴源表"""

    """sqoop执行命令"""
    src_tabfull = src_db + "." + src_tabname
    if srcdb_type == 'MYSQL' or srcdb_type == 'SQLSERVER':
        src_tabfull = src_tabname   # mysql和sqlserver不用加用户名前缀，不用类似pub_sys.table_name的形式
    tgt_tabfull = HDFS_LOCATION + "/stage_" + tgt_db + ".db" + "/" + tgt_tabname
    cmd = "%s import %s " % (CONF.get("conf", "run_bin"), conn_str) + " \\" + "\n"
    cmd += "--bindir %s" % (CONF.get("conf", "sqoop_bindir")) + " \\" + "\n"
    cmd += "--table %s --target-dir %s" % (src_tabfull, tgt_tabfull) + " \\" + "\n"
    cmd += "--fields-terminated-by '\\0001' \\" + "\n"
    cmd += "--lines-terminated-by '\\n' \\" + "\n"
    cmd += "--hive-drop-import-delims \\" + "\n"
    cmd += "--null-string '\\\\N' \\" + "\n"
    cmd += "--null-non-string '\\\\N' \\" + "\n"
    if (table_special_field != None) and (table_special_field.strip() != ''):
        cmd += set_special_field_deal(table_special_field)
    cmd += "-m 1 \\" + "\n"
    cmd += "--delete-target-dir  -- --default-character-set=utf-8"
    LOGGER.info("加载全量数据：" + cmd)
    return cmd


def set_special_field_deal(table_special_field):
    """对特殊字符进行字段类型转换为string类型"""
    special_str = ''
    if table_special_field.find(','):
        for value in table_special_field.split(','):
            special_str += "--map-column-java \"" + value + "=String\" \\" + "\n"
    else:
        special_str = "--map-column-java \"" + table_special_field + "=String\" \\" + "\n"
    return special_str


def get_sqoop_inc_table(srcdb_type, conn_str, src_db, src_tabname, tgt_db, tgt_tabname, incr_colname, incr_kscoltime,
                        incr_jscoltime, incr_coltime_mode, table_special_field):
    """处理增量数据到贴源表"""
    """sqoop执行命令"""
    #  处理Oracle数据源
    if srcdb_type == "ORACLE":
        if get_column_type(src_tabname, incr_colname):
            incr_colname = "TO_CHAR(" + incr_colname + ",'YYYYMMDD')"

        src_tabfull = src_db + "." + src_tabname
        tgt_tabfull = HDFS_LOCATION + "/stage_" + tgt_db + ".db" + "/" + tgt_tabname

        if incr_coltime_mode == 1:
            query_str = "\"SELECT * from %s where %s BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        elif incr_coltime_mode == 2:
            query_str = "\"SELECT * from %s where to_char(TO_DATE(%s,'yyyy-mm-dd'),'yyyymm') BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        elif incr_coltime_mode == 3:
            query_str = "\"SELECT * from %s where to_char(TO_DATE(%s,'yyyy-mm-dd'),'yyyy') BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        else:
            query_str = "\"SELECT * from %s where %s BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)

    #  处理MySQL数据源  date_format(%s,\'%%%%Y%%%%m%%%%d\')
    if srcdb_type == "MYSQL":
        src_tabfull = src_tabname
        tgt_tabfull = HDFS_LOCATION + "/stage_" + tgt_db + ".db" + "/" + tgt_tabname
        if incr_coltime_mode == 1:
            query_str = "\"SELECT * from %s where date_format(%s,\'%%Y%%m%%d\') BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        elif incr_coltime_mode == 2:
            query_str = "\"SELECT * from %s where date_format(%s,\'%%Y%%m\') BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        elif incr_coltime_mode == 3:
            query_str = "\"SELECT * from %s where date_format(%s,\'%%Y\') BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        else:
            query_str = "\"SELECT * from %s where date_format(%s,\'%%Y%%m%%d\') BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)

    #  处理SQLSERVER数据源  convert(varchar, getdate(), 112)
    if srcdb_type == "SQLSERVER":
        src_tabfull = src_tabname
        tgt_tabfull = HDFS_LOCATION + "/stage_" + tgt_db + ".db" + "/" + tgt_tabname
        if incr_coltime_mode == 1:
            query_str = "\"SELECT * from %s where convert(varchar, %s, 112) BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        elif incr_coltime_mode == 2:
            query_str = "\"SELECT * from %s where convert(varchar, %s, 112)/100 BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        elif incr_coltime_mode == 3:
            query_str = "\"SELECT * from %s where convert(varchar, %s, 112)/10000 BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)
        else:
            query_str = "\"SELECT * from %s where convert(varchar, %s, 112) BETWEEN  %s and  %s  and \\$CONDITIONS \"" % (
                src_tabfull, incr_colname, incr_kscoltime, incr_jscoltime)


    cmd = "%s import %s " % (CONF.get("conf", "run_bin"), conn_str) + " \\" + "\n"
    cmd += "--target-dir %s" % (tgt_tabfull) + " \\" + "\n"
    cmd += "--query %s " % (query_str) + " \\" + "\n"
    cmd += "--fields-terminated-by '\\0001' \\" + "\n"
    cmd += "--lines-terminated-by '\\n' \\" + "\n"
    cmd += "--hive-drop-import-delims \\" + "\n"
    cmd += "--null-string '\\\\N' \\" + "\n"
    cmd += "--null-non-string '\\\\N' \\" + "\n"
    if (table_special_field != None) and (table_special_field.strip() != ''):
        cmd += set_special_field_deal(table_special_field)
    cmd += "-m 1 \\" + "\n"
    cmd += "--delete-target-dir  -- --default-character-set=utf-8  "
    print(cmd)
    return cmd


def get_ods_table(ods_db, incr_colname, src_tabname, ods_tbl, cols, dt_ksvalue, dt_value, dt_mode):
    """生成ODS表数据"""
    stage_tbl_str = 'stage_' + ods_db + "." + ods_tbl
    ods_tbl_str = ods_db + "." + ods_tbl
    dt = dt_value
    incr_colname_temp = incr_colname
    # 如果增量字段为None或''和模式是按月分区，按年分区 就按传入的日期作为分区
    # if (incr_colname == None) or (incr_colname == '') or (dt_mode == 2) or (dt_mode == 3):
    #     if dt_mode == 2 and (incr_colname != None) and (incr_colname != ''):
    #         incr_colname = 'cast(' + incr_colname + '/100 as int)'
    #     elif dt_mode == 3 and (incr_colname != None) and (incr_colname != ''):
    #         incr_colname = 'cast(' + incr_colname + '/10000 as int)'
    #     else:
    #         incr_colname = "'" + dt_value + "'"

    if dt_mode == 1 and (incr_colname != None) and (incr_colname != ''):
        incr_colname = "substring(replace(cast(" + incr_colname + " as string),'-',''),1,8)"
    elif dt_mode == 2 and (incr_colname != None) and (incr_colname != ''):
        incr_colname = "substring(replace(cast(" + incr_colname + " as string),'-',''),1,6)"
    elif dt_mode == 3 and (incr_colname != None) and (incr_colname != ''):
        incr_colname = "substring(replace(cast(" + incr_colname + " as string),'-',''),1,4)"
    else:
        incr_colname = "'" + dt_value + "'"

    if CONF.get('conf', 'platform') == 'tdh':
        if dt_mode == 0:  # 不分区，直接插入即可
            sql = "INSERT OVERWRITE TABLE %s(%s) SELECT %s FROM %s t" % (
                ods_tbl_str, '\`' + str(cols).replace(',', '\`,\`') + '\`',
                '\`' + str(cols).replace(',', '\`,\`') + '\`', stage_tbl_str)
        else:
            if (incr_colname_temp is None) or (incr_colname_temp == ''):
                jyrlist = get_jyr_list(dt_ksvalue, dt_value, dt_mode)
                sql = ""
                for jyr in jyrlist:
                    sql += " INSERT OVERWRITE TABLE %s partition(dt=%s) (%s) SELECT %s FROM %s t;" % (
                        ods_tbl_str, jyr['JYR'], '\`' + str(cols).replace(',', '\`,\`') + '\`',
                        '\`' + str(cols).replace(',', '\`,\`') + '\`', stage_tbl_str
                    )
            else:
                # 为适应历史补采，此处开启动态分区，进行动态插入
                sql = "SET hive.exec.dynamic.partition=true; "
                if get_column_type(src_tabname, incr_colname_temp):
                    sql += " INSERT OVERWRITE TABLE %s partition(dt) (%s) SELECT %s,from_unixtime(unix_timestamp(cast(%s as string)), 'yyyyMMdd') as DT FROM %s t" % (
                        ods_tbl_str, '\`' + str(cols).replace(',', '\`,\`') + '\`',
                        '\`' + str(cols).replace(',', '\`,\`') + '\`', incr_colname, stage_tbl_str
                    )
                else:
                    sql += " INSERT OVERWRITE TABLE %s partition(dt) (%s) SELECT %s,cast(%s as string) as DT FROM %s t" % (
                        ods_tbl_str, '\`' + str(cols).replace(',', '\`,\`') + '\`',
                        '\`' + str(cols).replace(',', '\`,\`') + '\`', incr_colname, stage_tbl_str
                    )
                    # TODO: 插入语句循环的话会很长，需要优化
        LOGGER.info("Inceptor插入语句：%s" % sql)
    else:
        if dt_mode == 0:
            sql = "INVALIDATE METADATA %s;INVALIDATE METADATA %s;INSERT OVERWRITE TABLE %s(%s)  SELECT %s FROM %s t" % (
                stage_tbl_str, ods_tbl_str, ods_tbl_str, '\`' + str(cols).replace(',', '\`,\`') + '\`',
                '\`' + str(cols).replace(',', '\`,\`') + '\`', stage_tbl_str)
        else:
            # MySQL的date类型在hive是存为string类型，不能根据src_tabname判断而使用from_unixtime做转化
            # if get_column_type(src_tabname, incr_colname_temp):
            #     # 如果自增字段类型是 date类型 对该类型的内容进行格式转换为yyyyMMdd格式
            #     sql = "INVALIDATE METADATA %s;INVALIDATE METADATA %s;INSERT OVERWRITE TABLE %s(%s) partition(dt) SELECT %s,from_unixtime(unix_timestamp(cast(%s as string)), 'yyyyMMdd') as DT FROM %s t" % (
            #         stage_tbl_str, ods_tbl_str, ods_tbl_str, '\`' + str(cols).replace(',', '\`,\`') + '\`',
            #         '\`' + str(cols).replace(',', '\`,\`') + '\`', incr_colname, stage_tbl_str)

            sql = "INVALIDATE METADATA %s;INVALIDATE METADATA %s;INSERT OVERWRITE TABLE %s(%s) partition(dt) SELECT %s,%s as DT FROM %s t" % (
                stage_tbl_str, ods_tbl_str, ods_tbl_str, '\`' + str(cols).replace(',', '\`,\`') + '\`',
                '\`' + str(cols).replace(',', '\`,\`') + '\`', incr_colname, stage_tbl_str)
        LOGGER.info("IMPALA 插入语句：%s" % sql)
    return sql


def get_impala_runenv():
    """获取Impala运行环境的命令串"""
    impala_bin = CONF.get("conf", "impala_bin")
    if impala_bin.count("%s") == 1:
        impala_bin = impala_bin % IMPALA_HOSTNODE["HOST_IP"]
    elif impala_bin.count("%s") == 2:
        impala_bin = impala_bin % (IMPALA_HOSTNODE["HOST_IP"], IMPALA_HOSTNODE["USER_NAME"])
    elif impala_bin.count("%s") == 3:
        impala_bin = impala_bin % (IMPALA_HOSTNODE["HOST_IP"], IMPALA_HOSTNODE["USER_NAME"],
                                   des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                       base64.b64decode(IMPALA_HOSTNODE["PASSWORD"])).decode('utf-8', errors='ignore'))
    return impala_bin


def get_impala_cmd(run_sql):
    """生成impala执行命令"""
    """impala执行命令"""
    impala_bin = get_impala_runenv()
    cmd = "%s -q \"%s\"" % (impala_bin, run_sql)
    return cmd


def get_hive_runenv():
    """获取hive连接串
    注：因inceptor连接串与hive一致，因此可用来获取inceptor连接串
    :return:cmd_str
    """
    hive_bin = CONF.get('conf', 'hive_bin')
    if hive_bin.count("%s") == 2:
        hive_bin = hive_bin % (DB_CONF.get('hive', 'user'),
                               des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                   base64.b64decode(DB_CONF.get('hive', 'password'))).decode('utf-8', errors='ignore'))
    return hive_bin


def get_hive_cmd(run_sql):
    """
    生成Hive或Inceptor执行命令
    """
    hive_bin = get_hive_runenv()
    cmd_str = "%s -e \"%s\"" % (hive_bin, run_sql)
    return cmd_str


def get_file_info(file_name):
    """读取文件信息"""
    # TODO: 此处针对Inceptor输出的文件内容获取需要优化
    f = open(file_name)
    line = f.readline()
    col_list = []
    col_list_temp = []
    while line:
        if IS_TDH:
            col_list_temp = line.split(',')
        else:
            col_list = line.split()
        line = f.readline()
    f.close()
    if IS_TDH:
        for value in col_list_temp:
            col_list.append(value.strip().strip('"'))
    return col_list


def get_column_type(tabname, incr_colname):
    try:
        sType = False
        if incr_colname != '' and incr_colname != None:
            mysql_sql = "select column_type from pub_sys.t_ods_srcdb_tblcol where table_name=%s and column_name=%s limit 1";
            res = MYSQL.query(mysql_sql, (tabname, incr_colname))
            if str(res[0]['column_type']).upper().find('DATE') >= 0:
                sType = True
            else:
                sType = False
    except  Exception as e:
        global IS_EXCEPT
        IS_EXCEPT = 1
        LOGGER.error("get_column_type IS_EXCEPT err--->" + str(e))
        print(str(e))
        sType = False
    return sType


def get_rdbms_tbl_count(srcdb_type, srcdb, src_db, dbuser, tabname, incr_colname, incr_coltime, dt_mode,
                        incr_kscoltime):
    """获取关系型数据库的记录"""
    try:
        srctab_cnt_dict = {}
        tbl_cnt = []
        select_sql = ''
        where_sql = ''
        # Oracle数据源
        if srcdb_type == 'ORACLE':
            where_sql = ""
            select_sql = "select %s as RQ,%s as SRC_DB,'%s' as DB_USER,'%s' as TABLE_NAME,count(*) as CNT from %s.%s" % (
                incr_kscoltime, src_db, dbuser, tabname, dbuser, tabname)
            if incr_colname != '' and incr_colname is not None:
                if get_column_type(tabname, incr_colname):
                    incr_colname = "TO_CHAR(" + incr_colname + ",'YYYYMMDD')"
                print(dt_mode)
                if len(incr_kscoltime) == 6:
                    where_sql = " where  to_char(TO_DATE(%s,'yyyy-mm-dd'),'yyyymm')  BETWEEN %s and  %s " % (
                        incr_colname, incr_kscoltime, incr_coltime)
                elif len(incr_kscoltime) == 4:
                    where_sql = " where  to_char(TO_DATE(%s,'yyyy-mm-dd'),'yyyy')  BETWEEN %s and  %s " % (
                        incr_colname, incr_kscoltime, incr_coltime)
                else:
                    where_sql = " where  %s  BETWEEN %s and  %s " % (incr_colname, incr_kscoltime, incr_coltime)
                select_sql = select_sql + where_sql
            LOGGER.info("get_rdbms_tbl_count get source table count sql:" + select_sql)

            # 处理Oracle的jdbc连接串
            jdbc_url = srcdb['SRC_DB']
            if jdbc_url.find('/') > -1:
                # jdbc中存在/，为SERVICE_NAME
                dsn_str = cx_Oracle.makedsn(srcdb["HOST_IP"], srcdb["HOST_PORT"], service_name = srcdb["SRC_DATABASE_NAME"])
            else:
                # jdbc中不存在/，为SID
                dsn_str = cx_Oracle.makedsn(srcdb["HOST_IP"], srcdb["HOST_PORT"], sid = srcdb["SRC_DATABASE_NAME"])

            ORACLE = OracleUtil(dsn_str, True, srcdb["SRC_USER_NAME"], des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                               base64.b64decode(srcdb["SRC_PASSWORD"])).decode('utf-8',
                                                                                               errors='ignore'))
            print(select_sql)
            res = ORACLE.query(select_sql, {});
            tbl_cnt = res[0]
            ORACLE.close()

        # MySQL数据源
        if srcdb_type == 'MYSQL':
            sql = "select %s as RQ,%s as SRC_DB,'%s' as DB_USER,'%s' as TABLE_NAME,count(*) as CNT from %s.%s" % (
            incr_kscoltime, src_db, dbuser, tabname, dbuser, tabname)
            if incr_colname != '' and incr_colname is not None:
                if len(incr_kscoltime) == 8:    # 按日分区
                    where_sql = " where date_format(%s,\'%%%%Y%%%%m%%%%d\')  BETWEEN %s and  %s " % (incr_colname, incr_kscoltime, incr_coltime)
                if len(incr_kscoltime) == 6:    # 按月分区
                    where_sql = " where date_format(%s,\'%%%%Y%%%%m\')  BETWEEN %s and  %s " % (incr_colname, incr_kscoltime, incr_coltime)
                if len(incr_kscoltime) == 4:    # 按年分区
                    where_sql = " where date_format(%s,\'%%%%Y\')  BETWEEN %s and  %s " % (incr_colname, incr_kscoltime, incr_coltime)
            select_sql = sql + where_sql
            LOGGER.info("get_rdbms_tbl_count get source table count sql:" + select_sql)
            # 连接MySQL数据源执行sql语句
            src_mysql = MysqlUtil(srcdb["HOST_IP"], srcdb["SRC_USER_NAME"],
                                  des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                      base64.b64decode(srcdb["SRC_PASSWORD"])).decode('utf-8', errors='ignore'),
                srcdb["SRC_DATABASE_NAME"],
                srcdb["HOST_PORT"])
            res = src_mysql.query(select_sql, ())
            tbl_cnt = res[0]
            LOGGER.info(tbl_cnt)

        # SQLSERVER数据源
        if srcdb_type == 'SQLSERVER':
            sql = "select %s as RQ,%s as SRC_DB,'%s' as DB_USER,'%s' as TABLE_NAME,count(*) as CNT from %s.%s" % (
            incr_kscoltime, src_db, dbuser, tabname, dbuser, tabname)
            # -- format date or string or number as yyyymmdd
            # sqlserver的convert ( data_type, expression, style_id)函数，style_id=112时，格式是yyyymmdd
            # select  convert(varchar, getdate(), 112)
            if incr_colname != '' and incr_colname is not None:
                if len(incr_kscoltime) == 8:    # 按日分区
                    where_sql = " where convert(varchar, %s, 112) BETWEEN %s and  %s " % (incr_colname, incr_kscoltime, incr_coltime)
                if len(incr_kscoltime) == 6:    # 按月分区
                    where_sql = " where convert(varchar, %s, 112)/100 BETWEEN %s and  %s " % (incr_colname, incr_kscoltime, incr_coltime)
                if len(incr_kscoltime) == 4:    # 按年分区
                    where_sql = " where convert(varchar, %s, 112)/10000 BETWEEN %s and  %s " % (incr_colname, incr_kscoltime, incr_coltime)
            select_sql = sql + where_sql
            LOGGER.info("get_rdbms_tbl_count get source table count sql:" + select_sql)
            # 连接MySQL数据源执行sql语句
            src_sqlserver = SqlserverUtil(srcdb["HOST_IP"], srcdb["SRC_USER_NAME"],
                                  des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                      base64.b64decode(srcdb["SRC_PASSWORD"])).decode('utf-8', errors='ignore'),
                srcdb["SRC_DATABASE_NAME"],
                srcdb["HOST_PORT"])
            res = src_sqlserver.query(select_sql, ())
            tbl_cnt = res[0]
            LOGGER.info(tbl_cnt)

        srctab_cnt_dict['RQ'] = tbl_cnt["RQ"]
        srctab_cnt_dict['SRC_DB'] = tbl_cnt["SRC_DB"]
        srctab_cnt_dict['DB_USER'] = tbl_cnt["DB_USER"]
        srctab_cnt_dict['TABLE_NAME'] = tbl_cnt["TABLE_NAME"]
        srctab_cnt_dict['SOURCE_COUNT'] = tbl_cnt["CNT"]
        srctab_cnt_dict['SRC_CNT_CMD'] = select_sql
    except Exception as e:
        srctab_cnt_dict = None
        LOGGER.error("get_rdbms_tbl_count  error--->" + str(e))
    return srctab_cnt_dict


def get_ods_tbl_count(src_db, dbuser, tabname, ods_db, stage_table_name, ods_table_name, dt_ksvalue, dt_value, dt_mode,
                      incr_kscoltime, incr_coltime):
    """获取ODS层的记录数"""
    try:
        if IS_TDH:
            sh_bin = get_hive_runenv()
        else:
            sh_bin = get_impala_runenv()
        # 日志log目录下创建temp临时存放中间日志文件
        str = CONF.get("conf", "log_cnt_file")
        OSUtil.mkdir(sys.path[0] + str[:str.index('/', 5)])

        # 获取贴源层表数据记录数
        # 定义记录数临时输出文件名

        LOG_STAGE_CNT_FILE = (sys.path[0] + CONF.get("conf", "log_cnt_file")) % (
                ods_db + '_' + stage_table_name + "_stage")
        # print("LOG_STAGE_CNT_FILE------------------->"+LOG_STAGE_CNT_FILE)
        # OSUtil.mkdir(LOG_STAGE_CNT_FILE)
        select_sql = "select %s as RQ,%s as SRC_DB,'%s' as DB_USER,'%s' as TABLE_NAME,count(*) as CNT from %s.%s" % (
            incr_kscoltime, src_db, dbuser, tabname, 'stage_' + ods_db, stage_table_name)

        cmd = "%s --outputformat=csv --showHeader=false --verbose=false -e \"%s\" > %s" % (
            sh_bin, select_sql, LOG_STAGE_CNT_FILE) if IS_TDH else "%s -B -q \"%s\" -o %s" % (
            sh_bin, select_sql, LOG_STAGE_CNT_FILE)
        LOGGER.info("get stage table count sql:" + cmd)
        ProcUtil().single_pro(cmd)

        col_list = get_file_info(LOG_STAGE_CNT_FILE)
        # print(col_list)
        # 将col_list转换成字典型col_dict
        col_name = ['RQ', 'SRC_DB', 'DB_USER', 'TABLE_NAME', 'STAGE_COUNT']
        # 如果获取不到，则认为记录数为0写入文件
        if len(col_list) == 0 or len(col_list) != 5:
            col_list = [EXEC_JSRQ, src_db, dbuser, tabname, 0]
            col_list_str = str(EXEC_JSRQ) + '\t' + str(src_db) + '\t' + dbuser + '\t' + tabname + '\t' + str(0)
            OSUtil.file_save(LOG_STAGE_CNT_FILE, col_list_str)
        stagetab_cnt_dict = dict(zip(col_name, col_list))
        stagetab_cnt_dict['STAGE_CNT_CMD'] = cmd

        # 获取ODS层表数据记录数
        # 定义记录数临时输出文件名
        LOG_CNT_FILE = (sys.path[0] + CONF.get("conf", "log_cnt_file")) % (ods_db + '_' + ods_table_name)
        select_sql = "select %s as RQ,%s as SRC_DB,'%s' as DB_USER,'%s' as TABLE_NAME,count(*) as CNT from %s.%s" % (
            EXEC_JSRQ, src_db, dbuser, tabname, ods_db, ods_table_name)
        where_sql = ''
        if dt_mode != 0:
            where_sql = " where dt BETWEEN  '%s' and  '%s'  " % (dt_ksvalue, dt_value)
        select_sql = select_sql + where_sql
        cmd = "%s --outputformat=csv --showHeader=false --verbose=false -e \"%s\" > %s" % (
            sh_bin, select_sql, LOG_CNT_FILE) if IS_TDH else "%s -B -q \"%s\" -o %s" % (
            sh_bin, select_sql, LOG_CNT_FILE)
        LOGGER.info("get ods table count sql:" + cmd)
        ProcUtil().single_pro(cmd)

        col_list = get_file_info(LOG_CNT_FILE)
        # 将col_list转换成字典型col_dict
        col_name = ['RQ', 'SRC_DB', 'DB_USER', 'TABLE_NAME', 'TARGET_COUNT']
        # 如果获取不到，则认为记录数为0写入文件
        if len(col_list) == 0 or len(col_list) != 5:
            col_list_str = str(EXEC_JSRQ) + '\t' + str(src_db) + '\t' + dbuser + '\t' + tabname + '\t' + str(0)
            OSUtil.file_save(LOG_CNT_FILE, col_list_str)
        odstab_cnt_dict = dict(zip(col_name, col_list))
        odstab_cnt_dict['ODS_CNT_CMD'] = cmd
        odstab_cnt_dict['STAGE_COUNT'] = stagetab_cnt_dict['STAGE_COUNT']
        odstab_cnt_dict['STAGE_CNT_CMD'] = stagetab_cnt_dict['STAGE_CNT_CMD']
    except Exception as e:
        odstab_cnt_dict = None
        LOGGER.error("get_ods_tbl_count  error--->" + str(e))
    return odstab_cnt_dict


def set_err_info(ID, steps):
    if steps == 0:
        step_err = "初始化错误"
    elif steps == 1:
        step_err = "连接oracle异常"
    elif steps == 2:
        step_err = "源系统采集数据异常(sqoop)"
    elif steps == 3:
        step_err = "缓冲层写入ODS层数据异常(impala)"
    elif steps == 4:
        step_err = "获取源表、缓冲层、ods层数据条数异常"
    sql = """UPDATE t_ods_srcdb_collect_monitor SET COMMENT=%s WHERE ID=%s"""
    MYSQL.execute_sql(sql, (step_err, ID))
    return steps

def execute_proc(srcdb_type, sqoop_conn, srcdb, tbl, bid, log_id):
    # 开始记录程序日志
    if log_id == '':
        id = start_ods_proc_logs(tbl, bid)
    else:
        id = log_id
        update_ods_proc_logs(tbl, bid, log_id)
    if id != 0:
        start = int(time.time())
        steps = 0
        try:
            def add_log(pid, returncode, out, errs):
                LOGGER.info("exec_cmd : pid:%s, returncode:%s" % (pid, returncode))
                if out is not None:
                    LOGGER.info("exec_cmd out: %s" % out.decode('utf8', errors='ignore'))
                if errs is not None:
                    LOGGER.info("exec_cmd err: %s" % errs.decode('utf8', errors='ignore'))

            def succ(pid, returncode, out, errs):
                add_log(pid, returncode, out, errs)
                # 执行成功记录节点日志
                end_ods_proc_logs(id, int(time.time()) - start, 2)

            def fail(pid, returncode, out, errs):
                add_log(pid, returncode, out, errs)
                # 执行失败记录节点日志
                end_ods_proc_logs(id, int(time.time()) - start, 3)
                # EXCEPTION=break 就结束程序
                if EXCEPTION == 'break':
                    sys.exit(1)

            def exceptfail(pid, returncode, out, errs):
                add_log(pid, returncode, out, errs)

            stage_table_name = tbl["ODS_TBL_NAME"]
            ods_table_name = tbl["ODS_TBL_NAME"]
            col_str = get_tab_cols_str(srcdb["ID"], tbl["DB_USER"], tbl["TABLE_NAME"])
            # conn_str=jdbc:mysql://192.168.4.223:3306/test?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC，需要根据采集接口替换掉里面的数据库名
            # 如果是MySQL类型的，需要将jdbc连接串中的数据库名替换成采集接口中的用户名
            sqoop_conn_temp = sqoop_conn
            if srcdb_type == 'MYSQL':
                mysql_database_name = "/" + tbl["DB_USER"]
                sqoop_conn_temp = sqoop_conn.replace("/" + srcdb["SRC_DATABASE_NAME"], mysql_database_name)
            print(sqoop_conn_temp)
            LOGGER.info("SQOOP中的JDBC连接串为：" + sqoop_conn_temp)

            dt_mode = tbl['DT_MODE']  # 分区模式：0|不分区;1|按日分区；2|按月分区；3|按年分区
            dt_ksvalue = EXEC_KSRQ
            dt_value = EXEC_JSRQ  # ODS分区默认按照日期分区
            if dt_mode == 1:
                dt_value = EXEC_JSRQ
                dt_ksvalue = EXEC_KSRQ
            if dt_mode == 2:
                dt_value = str(int(EXEC_JSRQ) // 100)
                dt_ksvalue = str(int(EXEC_KSRQ) // 100)
            if dt_mode == 3:
                dt_value = str(int(EXEC_JSRQ) // 10000)
                dt_ksvalue = str(int(EXEC_KSRQ) // 10000)
            if dt_mode is None:
                dt_value = EXEC_JSRQ
                dt_ksvalue = EXEC_KSRQ

            # COLLECT_STRATEGY:采集策略（1|增量;2|全量;3|实时）
            # incr_coltime增量时间类型:1|日;2|月;3|年，默认为1
            # 开始时间
            incr_kscoltime = EXEC_KSRQ
            # 结束时间
            incr_coltime = EXEC_JSRQ
            if tbl["COLLECT_STRATEGY"] == 1:
                # 增量处理
                incr_coltime_mode = tbl["INCR_TIMEDIM"]
                if incr_coltime_mode == 1:
                    incr_kscoltime = EXEC_KSRQ
                    incr_coltime = EXEC_JSRQ
                if incr_coltime_mode == 2:
                    incr_kscoltime = str(int(EXEC_KSRQ) // 100)
                    incr_coltime = str(int(EXEC_JSRQ) // 100)
                if incr_coltime_mode == 3:
                    incr_kscoltime = str(int(EXEC_KSRQ) // 10000)
                    incr_coltime = str(int(EXEC_JSRQ) // 10000)
                # 加载数据到贴源表脚本
                cmd_str = get_sqoop_inc_table(srcdb_type, sqoop_conn_temp, tbl["DB_USER"], tbl["TABLE_NAME"], tbl["ODS_DB_NAME"],
                                              stage_table_name, tbl["INCR_COLNAME"], incr_kscoltime, incr_coltime,
                                              incr_coltime_mode, tbl["SPECIAL_FIELD"])
                # 生成数据到ODS表脚本

                run_sql = get_ods_table(tbl["ODS_DB_NAME"], tbl["INCR_COLNAME"], tbl["TABLE_NAME"], ods_table_name,
                                        col_str,
                                        dt_ksvalue,
                                        dt_value,
                                        tbl["DT_MODE"])
                print('cmd_str 1--------------->' + cmd_str)
            if tbl["COLLECT_STRATEGY"] == 2:
                # 全量处理
                # 加载数据到贴源表脚本
                # 区间范围补数据情况，只采集结束日期

                cmd_str = get_sqoop_all_table(srcdb_type, sqoop_conn_temp, tbl["DB_USER"], tbl["TABLE_NAME"], tbl["ODS_DB_NAME"],
                                              stage_table_name, tbl["SPECIAL_FIELD"])
                # 生成数据到ODS表脚本
                run_sql = get_ods_table(tbl["ODS_DB_NAME"], tbl["INCR_COLNAME"], tbl["TABLE_NAME"], ods_table_name,
                                        col_str,
                                        dt_ksvalue, dt_value,
                                        tbl["DT_MODE"])
                dt_ksvalue = dt_value

            # print('cmd_str 2--------------->' + cmd_str)
            # id = start_ods_proc_logs(tbl, cmd_str, run_sql)

            # 获取源数据库当前表记录数目，若记录为空，则不执行sqoop命令，直接结束执行
            steps = 1
            srctab_cnt_dict = get_rdbms_tbl_count(srcdb_type, srcdb, tbl['SRC_DB'], tbl["DB_USER"],
                                                  tbl["TABLE_NAME"],
                                                  tbl["INCR_COLNAME"], incr_coltime, tbl["DT_MODE"], incr_kscoltime)
            steps = 2
            if srctab_cnt_dict == None:
                message = "源表：" + tbl["TABLE_NAME"] + " 获取记录数异常"
                errs = None
                fail('0000', '0000', message.encode('utf8', errors='ignore'), errs)
                LOGGER.error("获取源表记录数异常，确定表是否存在")
                return
            if srctab_cnt_dict['SOURCE_COUNT'] != 0:
                # 执行SQOOP命令
                # print(cmd_str)
                LOGGER.info(cmd_str)
                ProcUtil().single_pro(cmd_str, add_log, fail)
                steps = 3
                # 执行脚本
                if CONF.get('conf', 'platform') == 'tdh':
                    # 执行Inceptor脚本
                    scripts = get_hive_cmd(run_sql)
                else:
                    scripts = get_impala_cmd(run_sql)
                # print(scripts)
                LOGGER.info(scripts)
                ProcUtil().single_pro(scripts, succ, fail)
                steps = 4
            # 记录数比对
            else:
                # 对于新建的表且数据条数为0的情况，刷新表
                scripts = get_impala_cmd(
                    "INVALIDATE METADATA stage_" + tbl["ODS_DB_NAME"] + "." + tbl["ODS_TBL_NAME"] + ";")
                LOGGER.info("ods缓冲层 scripts:---->" + scripts)
                ProcUtil().single_pro(scripts)
                scripts = get_impala_cmd("INVALIDATE METADATA " + tbl["ODS_DB_NAME"] + "." + tbl["ODS_TBL_NAME"] + ";")
                LOGGER.info("ods层 scripts:---->" + scripts)
                ProcUtil().single_pro(scripts)
                # 直接记录执行结束
                message = "源表：" + tbl["TABLE_NAME"] + " 数据条数为0，无需执行sqoop导入"
                errs = None
                succ('0000', '0000', message.encode('utf8', errors='ignore'), errs)

            odstab_cnt_dict = get_ods_tbl_count(tbl['SRC_DB'], tbl["DB_USER"], tbl["TABLE_NAME"], tbl["ODS_DB_NAME"],
                                                stage_table_name, ods_table_name, dt_ksvalue, dt_value, tbl["DT_MODE"],
                                                incr_kscoltime,
                                                incr_coltime)
            steps = 5
            # 合并源记录数和ODS记录数到同一个字典变量中
            tab_cnt_dict = odstab_cnt_dict
            tab_cnt_dict['SOURCE_COUNT'] = srctab_cnt_dict['SOURCE_COUNT']
            tab_cnt_dict['SRC_CNT_CMD'] = srctab_cnt_dict['SRC_CNT_CMD']

            # print(odstab_cnt_dict)
            # 更新记录数相关信息
            upd_ods_proc_logs(id, tab_cnt_dict, cmd_str, run_sql)
            return "done table:" + str(tbl["TABLE_NAME"])+"---id--->"+str(id)
        except Exception as e:
            message = "源表：" + tbl["TABLE_NAME"] + " 获取数据异常 --->"
            message += set_err_info(id, steps)
            LOGGER.error("execute_proc 1 message:" + str(message) + "--error-->:" + str(e))
            return "Error 1 table:" + str(tbl["TABLE_NAME"]) + "---id---->" + str(id)
    else:
        try:
            update_collect_monitor_except_one(DATAFLOW_LOGS_ID, BID, tbl["TABLE_NAME"])
            return "Error 2 table:" + str(tbl["TABLE_NAME"]) + "---id---->" + str(id)
        except Exception as e:
            message = "源表：" + tbl["TABLE_NAME"] + " 获取数据异常 --->"
            LOGGER.error("execute_proc 2 message:" + str(message) + "--error-->:" + str(e))
            return "Error 3 table:" + str(tbl["TABLE_NAME"]) + "---id---->" + str(id)


def Bar(arg):
    print(arg)


def get_tabname(bid):
    """根据id获取数据源信息"""
    res = None
    sql = """
        SELECT GROUP_CONCAT(tab_id) as tblname FROM pub_sys.vw_t_ods_group_tab_list_dataflow WHERE BID=%s GROUP BY bid;
        """
    # print(sql % (id, id))
    res = MYSQL.query(sql, (bid))
    tblname = ""
    for re in res:
        tblname = re['tblname']
    return tblname


def start_proc_logs(PROC_NAME, PROC_DESC,LSH):
    """记录程序日志"""
    sql = """
    insert into t_etl_proc_log(
    `DETAIL_LOG_ID`,
    `PROC_DESC`,
    `PROC_NAME`,
    `STAT_DATE`,
    `START_TIME`,
    `STATUS`,
    `PROCESS_ID`
    )values(%s,%s,%s,%s,%s,%s,%s)
    """
    LOGGER.info(sql % (LSH, PROC_DESC, PROC_NAME, EXEC_KSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1, os.getpid()))
    id = MYSQL.execute_sql(sql, (LSH, PROC_DESC, PROC_NAME, EXEC_KSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1, os.getpid()))
    return id


def end_dataflow_logs(ID,STATUS=None):
    """修改节点日志"""
    if STATUS is None:
        sql = "UPDATE t_etl_dataflow_logs SET LOG_PATH=%s WHERE ID=%s"
        LOGGER.info(sql % (LOG_FILE, ID))
        MYSQL.execute_sql(sql, (LOG_FILE, ID))
    else:
        sql = "UPDATE t_etl_dataflow_logs SET STATUS=%s, LOG_PATH=%s  WHERE ID=%s"
        LOGGER.info(sql % (STATUS, LOG_FILE, ID))
        MYSQL.execute_sql(sql, (STATUS, LOG_FILE, ID))

def end_proc_logs(ID, start, STATUS):
    """修改程序日志"""
    COST_TIME = int(time.time()) - start
    sql = "UPDATE t_etl_proc_log SET END_TIME=%s,COST_TIME=%s,STATUS=%s WHERE ID=%s"
    LOGGER.info(sql % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, STATUS, ID))
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COST_TIME, STATUS, ID))

def main():
    # 根据BID，将TBLNAME=ALL 转化为 TBLNAME=100533,102121,31223....
    PROC_NAME="OdsDataCollect.py"
    PROC_DESC="ODS数据采集"
    LSH_ID =start_proc_logs(PROC_NAME,PROC_DESC,DATAFLOW_LOGS_ID)

    global TBLNAME
    if TBLNAME == 'ALL':
        TBLNAME = get_tabname(BID)
    srcdbs = get_srcdb_list(SRCDB_ID)
    print("srcdbs:%s" % srcdbs)
    global IS_EXCEPT
    print("DATAFLOW_LOGS_ID------------->" + DATAFLOW_LOGS_ID)
    UPDATE_DATAFLOW_LOGS_PATH(DATAFLOW_LOGS_ID)
    LOGGER.info("进程池的大小为：" + str(PROC_PARALLELISM))
    # 创建对接各个系统的ODS库--SRCDB_ID=33 --DBUSER=DATACENTER --TBLNAME=ALL
    sqoop_conn = ""
    srcdb = srcdbs[0]
    srcdb_type = srcdb["DATASORCE_TYPE"]
    # if srcdb_type == 'ORACLE':
        # srcdb_dblink = '%s/%s@%s:%s/%s' % (srcdb["SRC_USER_NAME"],
                                           # des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                               # base64.b64decode(srcdb["SRC_PASSWORD"])).decode('utf-8',
                                                                                               # errors='ignore'),
                                           # srcdb["HOST_IP"], srcdb["HOST_PORT"], srcdb["SRC_DATABASE_NAME"])
        # 根据获取的jdbc连接，判断使用的是SID还是SERVICE_NAME，直接传入srcdb信息，使用时判断
        # srcdb_dblink = srcdb 
    sqoop_conn = get_sqoop_connect(srcdb["SRC_DB"], srcdb["SRC_USER_NAME"],
                                   des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                       base64.b64decode(srcdb["SRC_PASSWORD"])).decode('utf-8', errors='ignore'))
    tbls = get_srdb_tbl_list(srcdb['ID'], DBUSER, TBLNAME)
    LOGGER.info("main tbls count:%s" % str(tbls))
    pool = multiprocessing.Pool(processes=int(PROC_PARALLELISM))
    result = []
    for tbl in tbls:
        try:
            result.append(pool.apply_async(execute_proc, args=(srcdb_type, sqoop_conn, srcdb, tbl, BID, '',)))
            # time.sleep(int(PROC_PARALLELISM))
        except Exception as e:
            LOGGER.error("main pool.apply_async error---tbl:" + str(tbl) + "--Error----->:" + str(e))
    pool.close()
    pool.join()
    for res in result:
        LOGGER.info("pool result:"+str(res.get()))
    LOGGER.info("ALL process done.")
    # 重新执行一遍异常的采集表
    update_collect_monitor_except(DATAFLOW_LOGS_ID, BID)
    tbls = get_collect_monitor_except(DATAFLOW_LOGS_ID, BID)
    for tbl in tbls:
        try:
            tbltmp = get_srdb_tbl_list(srcdb['ID'], tbl["DB_USER"], tbl["TABLE_NAME"])
            execute_proc(srcdb_type, sqoop_conn, srcdb, tbltmp[0], BID, tbl["id"])
        except Exception as e:
            LOGGER.error("main recollection error---tablename:" + str(tbl) + "--Error----->:" + str(e))
    tbls = get_collect_monitor_except(DATAFLOW_LOGS_ID, BID)
    if len(tbls) > 0:
        IS_EXCEPT = 1

    # 关闭连接
    # MYSQL.close()
    LOGGER.info("ALL task done.")
    start = int(time.time())
    if (IS_EXCEPT == 1):
        end_proc_logs(LSH_ID, start, 3)
        sys.exit(1)
    else:
        end_proc_logs(LSH_ID, start, 2)
        sys.exit(0)


if __name__ == '__main__':
    read_config()
    init_param()
    main()
