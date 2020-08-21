#!/usr/bin/python
# coding=utf-8
"""
@name sqoop执行器
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
from utils.DBUtil import OracleUtil
from utils.ProcUtil import ProcUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64
import multiprocessing
import re

# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/RunSqoop.conf"
CONF = None
MYSQL = None
ORACLE_EXT = None
MYSQL_EXT = None
# 流水号
LSH = None
# 个性参数
VAR = {}
# 调度类型
TYPE = None
# 操作类型
OPR_TYPE = None
# Sqoop分组ID
GROUP_ID = None
GROUP = {}
# 日志
LOGGER = None
LOG_FILE = None
# impala主机
IMPALA_HOSTNODE = None
# hive主机
HIVE_HOSTNODE = None
# 开始日期
KSRQ = None
# 结束日期
JSRQ = None
# 本地推送id
PUSH_STATUS_ID = 0


def show_help():
    """指令帮助"""
    print("""
    -t                    调度类型
    -v                    个性参数
    -l                    流水号
    --opr_type            操作类型
    --group_id            Sqoop分组ID
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
    if OPR_TYPE is None:
        print("please input --opr_type")
        LOGGER.info("please input --opr_type")
        sys.exit(1)
    if GROUP_ID is None:
        print("please input --group_id")
        LOGGER.info("please input --group_id")
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
    # -t 1 -l 102 -v KSRQ=20180101 --opr_type=1 --group_id=1
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:", ["help", "type=", "var=", "lsh=", "opr_type=", "group_id="])
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
        if name in ("--opr_type",):
            global OPR_TYPE
            OPR_TYPE = value
        if name in ("--group_id",):
            global GROUP_ID
            GROUP_ID = value
    validate_input()


def get_sqoop_group(id):
    """根据id获取数据处理程序"""
    sql = """
SELECT a.*,b.TYPE,b.HOST_IP,b.HOST_PORT,b.USER_NAME,b.PASSWORD,b.DATABASE_NAME FROM t_job_sqoop_group a
LEFT JOIN t_srm_datasource b ON a.TAR_DB_ID=b.ID
WHERE a.ID=%s LIMIT 1
"""
    LOGGER.info(sql % id)
    res = MYSQL.query(sql, (id,))
    if len(res) > 0:
        return res[0]
    else:
        return None


def get_sqoop_proc(id):
    """根据id获取数据处理程序"""
    sql = """
SELECT a.ID,a.SRC_USER,a.DST_USER,a.C_TABLE,a.P_KEY,a.TYPE_TAB,a.OPR_DIMENSION,a.COL_DIMENSION,a.OPR_COLUMN,a.OPR_TYPE,a.SRC_DB_ID,a.DST_DB_ID,
b.DB_LINK AS SRC_DB,b.DATABASE_NAME AS SRC_DATABASE_NAME,b.TYPE AS SRC_TYPE,b.USER_NAME AS SRC_USER_NAME,b.PASSWORD AS SRC_PASSWORD,
IFNULL(c.TABLE_NAME,a.SRC_TAB) AS SRC_TAB,IFNULL(c.TABLE_NAME_CN,a.SRC_TAB) AS SRC_TAB_CN,
d.DB_LINK AS DST_DB,d.DATABASE_NAME AS DST_DATABASE_NAME,d.TYPE AS DST_TYPE,d.USER_NAME AS DST_USER_NAME,d.PASSWORD AS DST_PASSWORD,
IFNULL(e.TABLE_NAME,a.DST_TAB) AS DST_TAB,IFNULL(e.TABLE_NAME_CN,a.DST_TAB) AS DST_TAB_CN
FROM t_job_sqoop_proc a
LEFT JOIN t_srm_datasource b ON a.SRC_DB_ID=b.ID
LEFT JOIN t_table c ON (a.SRC_TAB=c.TABLE_NAME AND a.SRC_USER=c.OWNER)
LEFT JOIN t_srm_datasource d ON a.DST_DB_ID=d.ID
LEFT JOIN t_table e ON (a.DST_TAB=e.TABLE_NAME AND a.DST_USER=e.OWNER)
WHERE a.GROUP_ID=%s
"""
    if "PROC" in VAR:
        sql += " AND a.ID IN (%s)" % VAR["PROC"]
    LOGGER.info(sql % id)
    res = MYSQL.query(sql, (id,))
    return res


def get_impala_hostnode():
    """获取impala主机"""
    sql = """
        SELECT HOST_IP,USER_NAME,PASSWORD,DB_LINK FROM t_srm_datasource WHERE TYPE=5 LIMIT 1
    """
    LOGGER.info(sql)
    res = MYSQL.query(sql, ())
    if len(res) > 0:
        LOGGER.info("get_impala_hostnode: %s", res[0])
        return res[0]
    else:
        return None


def get_hive_hostnode():
    """获取hive主机"""
    sql = """
        SELECT HOST_IP,USER_NAME,PASSWORD,DB_LINK FROM t_srm_datasource WHERE TYPE=2 LIMIT 1
    """
    LOGGER.info(sql)
    res = MYSQL.query(sql, ())
    if len(res) > 0:
        LOGGER.info("get_hive_hostnode: %s", res[0])
        return res[0]
    else:
        return None


def get_table_columns(src_db, db_user, table_name):
    """获取表字段类型"""
    #  COLUMN_TYPE IN ('CLOB','BLOB') AND
    sql = """
        SELECT COLUMN_NAME,COLUMN_TYPE FROM t_ods_srcdb_tblcol WHERE FTYPE=2 AND SRC_DB=%s AND DB_USER=%s AND TABLE_NAME=%s
    """
    LOGGER.info(sql % (src_db, db_user, table_name))
    res = MYSQL.query(sql, (src_db, db_user, table_name))
    if len(res) > 0:
        return res
    else:
        return None


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
    id = MYSQL.execute_sql(sql, (
    LSH, PROC_DESC, PROC_NAME, KSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 1, TYPE))
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


def push_monitor(PROC_ID, SRC_DB, DB_USER, TABLE_NAME, ODS_DB_NAME, ODS_TBL_NAME, PUSH_TYPE):
    """添加校验日志"""
    sql = """
    INSERT INTO t_ods_srcdb_collect_monitor(SRC_DB,DB_USER,TABLE_NAME,ODS_DB_NAME,ODS_TBL_NAME,START_TIME,PUSH_TYPE,
    START_RUN_TIME,END_RUN_TIME,DATAFLOW_LOGS_ID,RELATION_ID,FTYPE,RUN_STATUS) 
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,2,1)
    """
    LOGGER.info(sql % (
    SRC_DB, DB_USER, TABLE_NAME, ODS_DB_NAME, ODS_TBL_NAME, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    PUSH_TYPE, KSRQ, JSRQ, GROUP_ID, PROC_ID))
    id = MYSQL.execute_sql(sql, (
        SRC_DB, DB_USER, TABLE_NAME, ODS_DB_NAME, ODS_TBL_NAME, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        PUSH_TYPE, KSRQ, JSRQ, GROUP_ID, PROC_ID))
    return id


def update_monitor(id, start, count, flag, fail=False):
    """修改校验日志"""
    if fail is True:
        END_TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        COST_TIME = int(time.time()) - start
        sql = """
        UPDATE t_ods_srcdb_collect_monitor SET SOURCE_COUNT=0,TARGET_COUNT=0,RUN_STATUS=3,END_TIME=%s,COST_TIME=%s where id=%s
        """
        LOGGER.info(sql % (END_TIME, COST_TIME, id))
        MYSQL.execute_sql(sql, (END_TIME, COST_TIME, id))
        return
    if flag is True:
        END_TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        COST_TIME = int(time.time()) - start
        sql = """
        UPDATE t_ods_srcdb_collect_monitor SET END_TIME=%s,TARGET_COUNT=%s,COST_TIME=%s where id=%s
        """
        LOGGER.info(sql % (END_TIME, count, COST_TIME, id))
        MYSQL.execute_sql(sql, (END_TIME, count, COST_TIME, id))
        sql = """
        UPDATE t_ods_srcdb_collect_monitor SET RUN_STATUS=2 where id=%s and SOURCE_COUNT=TARGET_COUNT
        """
        MYSQL.execute_sql(sql, id)
        sql = """
        UPDATE t_ods_srcdb_collect_monitor SET RUN_STATUS=3 where id=%s and SOURCE_COUNT!=TARGET_COUNT
        """
        MYSQL.execute_sql(sql, id)
    else:
        sql = """
        UPDATE t_ods_srcdb_collect_monitor SET SOURCE_COUNT=%s where id=%s
        """
        LOGGER.info(sql % (count, id))
        MYSQL.execute_sql(sql, (count, id))


def push_status():
    """添加推送日志"""
    sql = """
    INSERT INTO t_push_status(SRC_SYS,BUSI_BEGIN_DATE,BUSI_END_DATE,PUSH_BEGIN_DT,PUSH_STATUS) 
    values(%s,%s,%s,%s,1)
    """
    LOGGER.info(sql % (GROUP["NAME"], KSRQ, JSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    return MYSQL.execute_sql(sql, (GROUP["NAME"], KSRQ, JSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def update_push_status(id):
    """添加推送日志"""
    sql = """
    SELECT * FROM t_etl_proc_log WHERE DETAIL_LOG_ID=%s AND STATUS IN (3,4);
    """
    LOGGER.info(sql % LSH)
    res = MYSQL.query(sql, (LSH,))
    PUSH_STATUS = 0
    if len(res) > 0:
        PUSH_STATUS = -1
    sql = """
    UPDATE t_push_status SET PUSH_END_DT=%s,PUSH_STATUS=%s where PUSH_NO=%s
    """
    LOGGER.info(sql % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), PUSH_STATUS, id))
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), PUSH_STATUS, id))


def push_other_status(PUSH_STATUS_ID):
    """添加推送对方日志"""
    if GROUP["TYPE"] == 4 or GROUP["TYPE"] == "4":
        global ORACLE_EXT
        ORACLE_EXT = OracleUtil(
            GROUP["HOST_IP"],
            GROUP["USER_NAME"],
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                base64.b64decode(GROUP["PASSWORD"])).decode('utf-8', errors='ignore'),
            GROUP["DATABASE_NAME"],
            GROUP["HOST_PORT"]
        )
        sql = """
        INSERT INTO t_push_status(PUSH_NO,SRC_SYS,BUSI_BEGIN_DATE,BUSI_END_DATE,PUSH_BEGIN_DT,PUSH_STATUS)
        values(:1,:2,:3,:4,to_date(:5,'YYYY-MM-DD HH24:MI:SS'),1)
        """
        try:
            ORACLE_EXT.execute_sql(sql, (
            PUSH_STATUS_ID, GROUP["NAME"], KSRQ, JSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        except Exception as e:
            LOGGER.info("error: %s" % e)
    elif GROUP["TYPE"] == 1 or GROUP["TYPE"] == "1":
        global MYSQL_EXT
        MYSQL_EXT = MysqlUtil(
            GROUP["HOST_IP"],
            GROUP["USER_NAME"],
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                base64.b64decode(GROUP["PASSWORD"])).decode('utf-8', errors='ignore'),
            GROUP["DATABASE_NAME"],
            GROUP["HOST_PORT"]
        )
        sql = """
        INSERT INTO t_push_status(PUSH_NO,SRC_SYS,BUSI_BEGIN_DATE,BUSI_END_DATE,PUSH_BEGIN_DT,PUSH_STATUS)
        values(%s,%s,%s,%s,%s,1)
        """
        try:
            MYSQL_EXT.execute_sql(sql, (
            PUSH_STATUS_ID, GROUP["NAME"], KSRQ, JSRQ, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        except Exception as e:
            LOGGER.info("error: %s" % e)


def update_push_other_status(id):
    """添加推送日志"""
    sql = """
    SELECT * FROM t_etl_proc_log WHERE DETAIL_LOG_ID=%s AND STATUS IN (3,4);
    """
    res = MYSQL.query(sql, (LSH,))
    push_status = 0
    if len(res) > 0:
        push_status = -1
    if GROUP["TYPE"] == 4 or GROUP["TYPE"] == "4":
        sql = """
        UPDATE t_push_status SET PUSH_END_DT=to_date(:1,'YYYY-MM-DD HH24:MI:SS'),PUSH_STATUS=:2 where PUSH_NO=:3
        """
        try:
            ORACLE_EXT.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), push_status, id))
        except Exception as e:
            LOGGER.info("error: %s" % e)
    elif GROUP["TYPE"] == 1 or GROUP["TYPE"] == "1":
        sql = """
        UPDATE t_push_status SET PUSH_END_DT=%s,PUSH_STATUS=%s where PUSH_NO=%s
        """
        try:
            MYSQL_EXT.execute_sql(sql, (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), push_status, id))
        except Exception as e:
            LOGGER.info("error: %s" % e)


def is_stop():
    """是否终止执行程序"""
    sql = "SELECT ID FROM t_etl_proc_log WHERE DETAIL_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=4"
    LOGGER.info(sql % (LSH, TYPE))
    res = MYSQL.query(sql, (LSH, TYPE))
    LOGGER.info(res)
    return len(res) > 0


def end_dataflow_logs(STATUS=None):
    """修改节点日志"""
    if STATUS is None:
        sql = "UPDATE t_etl_dataflow_logs SET LOG_PATH=%s WHERE ID=%s"
        LOGGER.info(sql % (LOG_FILE, LSH))
        MYSQL.execute_sql(sql, (LOG_FILE, LSH))
    else:
        sql = "UPDATE t_etl_dataflow_logs SET STATUS=%s, LOG_PATH=%s WHERE ID=%s"
        LOGGER.info(sql % (STATUS, LOG_FILE, LSH))
        MYSQL.execute_sql(sql, (STATUS, LOG_FILE, LSH))


def execute_proc(proc):
    """sqoop执行命令"""
    if is_stop() is True:
        return
    LOGGER.info("proc: %s", proc)
    id = start_proc_logs(proc["SRC_TAB"], proc["SRC_TAB_CN"])
    start = int(time.time())
    succ_flag = True

    where_value = ""
    where_except = ""
    if proc["OPR_DIMENSION"] == 1:
        """年"""
        where_value = proc["COL_DIMENSION"] + " BETWEEN " + KSRQ[0:4] + " AND " + JSRQ[0:4]
        where_except = proc["COL_DIMENSION"] + " < " + KSRQ[0:4] + " AND " + proc["COL_DIMENSION"] + " > " + JSRQ[0:4]
    elif proc["OPR_DIMENSION"] == 2:
        """月"""
        where_value = proc["COL_DIMENSION"] + " BETWEEN " + KSRQ[0:6] + " AND " + JSRQ[0:6]
        where_except = proc["COL_DIMENSION"] + " < " + KSRQ[0:6] + " AND " + proc["COL_DIMENSION"] + " > " + JSRQ[0:6]
    elif proc["OPR_DIMENSION"] == 3:
        """日"""
        where_value = proc["COL_DIMENSION"] + " BETWEEN " + KSRQ + " AND " + JSRQ
        where_except = proc["COL_DIMENSION"] + " < " + KSRQ + " AND " + proc["COL_DIMENSION"] + " > " + JSRQ

    def add_log(pid, returncode, outs, errs):
        LOGGER.info("exec_cmd id:%s,  pid:%s, returncode:%s" % (id, pid, returncode))
        LOGGER.info("exec_cmd outs: %s" % outs)
        LOGGER.info("exec_cmd errs: %s" % errs)
        if returncode == -9:
            end_proc_logs(id, start, 4)

    def succ():
        # 执行成功记录节点日志
        end_proc_logs(id, start, 2)

    def fail(returncode):
        # 执行失败记录节点日志
        if returncode == -9:
            end_proc_logs(id, start, 4)
        else:
            end_proc_logs(id, start, 3)

    def before(pid):
        # 进程运行前操作
        update_proc_logs(id, pid)

    cmd = ""
    global HIVE_HOSTNODE
    HIVE_HOSTNODE = get_hive_hostnode()

    hive_bin = CONF.get("conf", "hive_bin")
    if hive_bin.count("%s") == 1:
        hive_bin = hive_bin % HIVE_HOSTNODE["DB_LINK"]
    elif hive_bin.count("%s") == 3:
        hive_bin = hive_bin % (HIVE_HOSTNODE["DB_LINK"], HIVE_HOSTNODE["USER_NAME"],
                               des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                   base64.b64decode(HIVE_HOSTNODE["PASSWORD"])).decode('utf-8', errors='ignore'))

    impala_bin = hive_bin
    if CONF.get("conf", "platform") == "cdh":
        global IMPALA_HOSTNODE
        IMPALA_HOSTNODE = get_impala_hostnode()
        impala_bin = CONF.get("conf", "impala_bin")
        if impala_bin.count("%s") == 1:
            impala_bin = impala_bin % IMPALA_HOSTNODE["HOST_IP"]
        elif impala_bin.count("%s") == 2:
            impala_bin = impala_bin % (IMPALA_HOSTNODE["HOST_IP"], IMPALA_HOSTNODE["USER_NAME"])
        elif impala_bin.count("%s") == 3:
            impala_bin = impala_bin % (IMPALA_HOSTNODE["HOST_IP"], IMPALA_HOSTNODE["USER_NAME"],
                                       des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                                           base64.b64decode(IMPALA_HOSTNODE["PASSWORD"])).decode('utf-8',
                                                                                                 errors='ignore'))

    LOGGER.info("hive_bin: %s" % hive_bin)
    LOGGER.info("impala_bin: %s" % impala_bin)
    map_column = ""

    LOGGER.info("OPR_TYPE: %s" % (OPR_TYPE == "2" or OPR_TYPE == 2))
    if OPR_TYPE == "1" or OPR_TYPE == 1:
        """导入"""
        src_db = proc["SRC_DB"]
        src_user = proc["SRC_USER"]
        src_tab = proc["SRC_TAB"]
        in_tab = src_tab
        opr_column = "*"
        opr_columns = "*"
        columns = ""
        insert_column = ""
        if proc["SRC_TYPE"] == 1:
            """mysql"""
            src_db = src_db.replace(proc["SRC_DATABASE_NAME"], proc["SRC_USER"])
        if proc["SRC_TYPE"] == 4:
            """oracle"""
            src_user = str(src_user).upper()
            src_tab = str(src_tab).upper()
            in_tab = "%s.%s" % (src_user, src_tab)
            table_columns = get_table_columns(proc["SRC_DB_ID"], src_user, src_tab)
            if table_columns:
                for column in table_columns:
                    if column["COLUMN_TYPE"] == 'CLOB' or column["COLUMN_TYPE"] == 'BLOB':
                        map_column += "%s=String," % column["COLUMN_NAME"]
                map_column = map_column.rstrip(",")
            map_column = str(map_column).lower()

        if proc["OPR_COLUMN"] is not None and proc["OPR_COLUMN"] != "":
            opr_column = proc["OPR_COLUMN"]
            if proc["SRC_TYPE"] == 4:
                """oracle"""
                opr_column = str(opr_column).upper()
            columns = "--columns \"%s\"" % opr_column
            opr_columns = opr_column
            if proc["OPR_TYPE"] == 3:
                opr_columns = "%s,%s" % (opr_column, proc["COL_DIMENSION"])
                columns = "--columns \"%s\"" % opr_columns

        if opr_column != "*":
            insert_column = "(%s)" % opr_column

        tmp_cmd = "%s -e \"drop table if exists temp.%s_import_temp purge;create table temp.%s_import_temp as select %s from %s.%s where 1=2;alter table temp.%s_import_temp set SERDEPROPERTIES('serialization.null.format' = '\\\\\\N');\"" % (
            hive_bin, proc["DST_TAB"], proc["DST_TAB"], opr_columns, proc["DST_USER"], proc["DST_TAB"], proc["DST_TAB"]
        )
        LOGGER.info(tmp_cmd)
        returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
        if returncode != 0:
            succ_flag = False
        if succ_flag is False or is_stop() is True:
            fail(returncode)
            return
        if proc["OPR_TYPE"] == 2:
            """普通增量导入"""
            cmd = "%s import --username %s --password %s --delete-target-dir --verbose" % (
            CONF.get("conf", "run_bin"), proc["SRC_USER_NAME"],
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                base64.b64decode(proc["SRC_PASSWORD"])).decode('utf-8', errors='ignore'))

            cmd += " --connect \"%s\" %s --table %s --null-string '\\\\N' --null-non-string '\\\\N' --hive-import --hive-table temp.%s_import_temp --where \"%s\" -m %s" % (
                src_db, columns, in_tab, proc["DST_TAB"], where_value, CONF.get("conf", "sqoop_num"))

            if map_column != "":
                cmd += " --map-column-java \"%s\"" % map_column
            cmd += " -- --default-character-set=utf-8 --direct"

            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            if CONF.get("conf", "platform") == "cdh":
                tmp_cmd = "%s -q \"INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s SELECT * FROM (SELECT %s FROM temp.%s_import_temp UNION ALL SELECT %s FROM %s.%s WHERE %s) T\"" % (
                    impala_bin, proc["DST_TAB"], proc["DST_USER"], proc["DST_TAB"], insert_column, opr_column,
                    proc["DST_TAB"], opr_column, proc["DST_USER"], proc["DST_TAB"], where_except
                )
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if succ_flag is False or is_stop() is True:
                    fail(returncode)
                    return
            elif CONF.get("conf", "platform") == "tdh":
                tmp_cmd = "%s -e \"INSERT OVERWRITE TABLE %s.%s%s SELECT * FROM (SELECT %s FROM temp.%s_import_temp UNION ALL SELECT %s FROM %s.%s WHERE %s) T\"" % (
                    impala_bin, proc["DST_USER"], proc["DST_TAB"], insert_column, opr_column,
                    proc["DST_TAB"], opr_column, proc["DST_USER"], proc["DST_TAB"], where_except
                )
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if succ_flag is False or is_stop() is True:
                    fail(returncode)
                    return
            else:
                end_proc_logs(id, start, 3)
        elif proc["OPR_TYPE"] == 3:
            """分区增量导入"""
            cmd = "%s import --username %s --password %s --delete-target-dir --verbose" % (
            CONF.get("conf", "run_bin"), proc["SRC_USER_NAME"],
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                base64.b64decode(proc["SRC_PASSWORD"])).decode('utf-8', errors='ignore'))

            cmd += " --connect \"%s\" %s --table %s --null-string '\\\\N' --null-non-string '\\\\N' --hive-import --hive-table temp.%s_import_temp --where \"%s\" -m %s" % (
                src_db, columns, in_tab, proc["DST_TAB"], where_value, CONF.get("conf", "sqoop_num"))

            if map_column != "":
                cmd += " --map-column-java \"%s\"" % map_column
            cmd += " -- --default-character-set=utf-8 --direct"

            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            if CONF.get("conf", "platform") == "cdh":
                tmp_cmd = "%s -q \"INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s PARTITION(%s) SELECT %s FROM temp.%s_import_temp\"" % (
                    impala_bin, proc["DST_TAB"], proc["DST_USER"], proc["DST_TAB"], insert_column,
                    proc["COL_DIMENSION"], opr_column, proc["DST_TAB"]
                )
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if succ_flag is False or is_stop() is True:
                    fail(returncode)
                    return
            elif CONF.get("conf", "platform") == "tdh":
                tmp_cmd = "%s -e \"INSERT OVERWRITE TABLE %s.%s PARTITION(%s) %s SELECT %s FROM temp.%s_import_temp\"" % (
                    impala_bin, proc["DST_USER"], proc["DST_TAB"], proc["COL_DIMENSION"], insert_column, opr_column,
                    proc["DST_TAB"]
                )
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if succ_flag is False or is_stop() is True:
                    fail(returncode)
                    return
            else:
                end_proc_logs(id, start, 3)
        elif proc["OPR_TYPE"] == 1:
            """全量导入"""
            cmd = "%s import --username %s --password %s --delete-target-dir --verbose" % (
            CONF.get("conf", "run_bin"), proc["SRC_USER_NAME"],
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                base64.b64decode(proc["SRC_PASSWORD"])).decode('utf-8', errors='ignore'))

            cmd += " --connect \"%s\" %s --table %s --null-string '\\\\N' --null-non-string '\\\\N' --hive-import --hive-table temp.%s_import_temp -m %s" % (
                src_db, columns, in_tab, proc["DST_TAB"], CONF.get("conf", "sqoop_num"))

            if map_column != "":
                cmd += " --map-column-java \"%s\"" % map_column
            cmd += " -- --default-character-set=utf-8 --direct"
            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            if CONF.get("conf", "platform") == "cdh":
                tmp_cmd = "%s -q \"INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s select %s from temp.%s_import_temp\"" % (
                    impala_bin, proc["DST_TAB"], proc["DST_USER"], proc["DST_TAB"], insert_column, opr_column,
                    proc["DST_TAB"]
                )
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if succ_flag is False or is_stop() is True:
                    fail(returncode)
                    return
            elif CONF.get("conf", "platform") == "tdh":
                tmp_cmd = "%s -e \"INSERT OVERWRITE %s.%s%s select %s from temp.%s_import_temp\"" % (
                    impala_bin, proc["DST_USER"], proc["DST_TAB"], insert_column, opr_column, proc["DST_TAB"]
                )
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if succ_flag is False or is_stop() is True:
                    fail(returncode)
                    return
            else:
                end_proc_logs(id, start, 3)

        tmp_cmd = "%s -e \"drop table if exists temp.%s_import_temp purge;\"" % (hive_bin, proc["DST_TAB"])
        LOGGER.info(tmp_cmd)
        returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
        if returncode != 0:
            succ_flag = False
        if succ_flag is False:
            fail(returncode)
            return
    elif OPR_TYPE == "2" or OPR_TYPE == 2:
        """导出"""
        dst_db = proc["DST_DB"]
        dst_user = proc["DST_USER"]
        dst_tab = proc["DST_TAB"]
        out_tab = dst_tab
        opr_columns = "*"
        tmp_columns = ""
        if proc["DST_TYPE"] == 1:
            """mysql"""
            dst_db = dst_db.replace(proc["DST_DATABASE_NAME"], proc["DST_USER"])
        if proc["DST_TYPE"] == 4:
            """oracle"""
            opr_columns = ""
            dst_user = str(dst_user).upper()
            dst_tab = str(dst_tab).upper()
            out_tab = "%s.%s" % (dst_user, dst_tab)
            table_columns = get_table_columns(proc["DST_DB_ID"], dst_user, dst_tab)
            if table_columns and len(table_columns) > 0:
                for column in table_columns:
                    if column["COLUMN_TYPE"] == 'CLOB' or column["COLUMN_TYPE"] == 'BLOB':
                        map_column += "%s=String," % column["COLUMN_NAME"]
                        opr_columns += "regexp_replace(trim(%s),' ','') as %s," % (
                        column["COLUMN_NAME"], column["COLUMN_NAME"])
                    else:
                        opr_columns += "%s," % column["COLUMN_NAME"]
                    tmp_columns += "%s," % column["COLUMN_NAME"]
            opr_columns = str(opr_columns.rstrip(",")).lower()
            map_column = str(map_column.rstrip(",")).lower()
            tmp_columns = str(tmp_columns.rstrip(",")).lower()

        if proc["OPR_COLUMN"] is not None and proc["OPR_COLUMN"] != "":
            map_column = ""
            opr_columns = ""
            tmp_columns = str(proc["OPR_COLUMN"]).lower()
            opr_columns_arr = str(proc["OPR_COLUMN"]).split(",")
            if proc["DST_TYPE"] == 4:
                """oracle"""
                table_columns = get_table_columns(proc["DST_DB_ID"], dst_user, dst_tab)
                if table_columns and len(table_columns) > 0:
                    for opr_column in opr_columns_arr:
                        tmp_flag = True
                        for column in table_columns:
                            if str(column["COLUMN_NAME"]).lower() == opr_column.strip().lower() and (
                                    column["COLUMN_TYPE"] == 'CLOB' or column["COLUMN_TYPE"] == 'BLOB'):
                                map_column += "%s=String," % str(column["COLUMN_NAME"]).lower()
                                opr_columns += "regexp_replace(trim(%s),' ','') as %s," % (
                                column["COLUMN_NAME"], column["COLUMN_NAME"])
                                tmp_flag = False
                        if tmp_flag is True:
                            opr_columns += "%s," % opr_column
                map_column = map_column.rstrip(",")
            else:
                opr_columns = str(proc["OPR_COLUMN"]).lower()
            if proc["OPR_TYPE"] == 3:
                opr_columns = "%s,%s" % (opr_columns, proc["COL_DIMENSION"])
            opr_columns = str(opr_columns).rstrip(",").lower()
        pattern = re.compile(r'[\"\']*\w+[\"\']*\sas\s')
        columns = "--columns \"%s\"" % re.sub(pattern, '', tmp_columns)
        if columns == "--columns \"\"":
            columns = ""
        if opr_columns == "":
            opr_columns = "*"

        monitor_id = push_monitor(proc["ID"], proc["DST_DB_ID"], dst_user, dst_tab, proc["SRC_USER"], proc["SRC_TAB"],
                                  2)

        def get_hive_count(pid, returncode, outs, errs):
            LOGGER.info("get_hive_count id:%s,  pid:%s, returncode:%s" % (id, pid, returncode))
            if returncode == 0 and outs and len(outs) > 0:
                ret_strs = str(outs).split("|")
                if len(ret_strs) > 3:
                    num = ret_strs[3].replace("\n", "").replace(" ", "")
                    update_monitor(monitor_id, start, num, False)
            if returncode != 0:
                update_monitor(monitor_id, start, 0, True, True)

        def get_rdbms_count(pid, returncode, outs, errs):
            LOGGER.info("get_rdbms_count id:%s,  pid:%s, returncode:%s" % (id, pid, returncode))
            if returncode == 0 and outs and len(outs) > 0:
                ret_strs = str(outs).split("|")
                if len(ret_strs) > 3:
                    num = ret_strs[3].replace("\n", "").replace(" ", "")
                    LOGGER.info(num)
                    update_monitor(monitor_id, start, num, True)
            if returncode != 0:
                update_monitor(monitor_id, start, 0, True, True)

        LOGGER.info("proc OPR_TYPE: %s" % proc["OPR_TYPE"])
        if proc["OPR_TYPE"] == 2 or proc["OPR_TYPE"] == "2" or proc["OPR_TYPE"] == "3" or proc["OPR_TYPE"] == 3:
            """增量导出"""
            tmp_cmd = "%s -e \"drop table if exists temp.%s_export_temp purge;create table temp.%s_export_temp as select %s from %s.%s where %s\"" % (
                hive_bin, proc["SRC_TAB"], proc["SRC_TAB"], opr_columns, proc["SRC_USER"], proc["SRC_TAB"], where_value
            )
            LOGGER.info(tmp_cmd)
            returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
                update_monitor(monitor_id, start, 0, True, True)
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            tmp_cmd = "%s eval --connect \"%s\" --username %s --password %s --verbose --query \"delete from %s.%s where %s\"" % (
                CONF.get("conf", "run_bin"), dst_db, proc["DST_USER_NAME"],
                des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                    base64.b64decode(proc["DST_PASSWORD"])).decode('utf-8', errors='ignore'),
                dst_user, dst_tab, where_value
            )
            LOGGER.info(tmp_cmd)
            returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
                update_monitor(monitor_id, start, 0, True, True)
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            cmd += "%s export --username %s --password %s --verbose" % (
            CONF.get("conf", "run_bin"), proc["DST_USER_NAME"],
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                base64.b64decode(proc["DST_PASSWORD"])).decode('utf-8', errors='ignore'))

            cmd += " --connect \"%s\" %s --table %s --hcatalog-database temp --hcatalog-table %s_export_temp -m %s" % (
                dst_db, columns, out_tab, proc["SRC_TAB"], CONF.get("conf", "sqoop_num"))

            if map_column != "":
                cmd += " --map-column-java \"%s\"" % map_column
            cmd += " -- --default-character-set=utf-8 --delete-target-dir --direct"

            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
                update_monitor(monitor_id, start, 0, True, True)
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            tmp_cmd = "%s -e \"SELECT count(1) FROM temp.%s_export_temp\"" % (hive_bin, proc["SRC_TAB"])
            LOGGER.info(tmp_cmd)
            ProcUtil().single_pro(tmp_cmd, get_hive_count, get_hive_count)

            tmp_cmd = "%s eval --connect \"%s\" --username %s --password %s --verbose --query \"select count(1) from %s.%s where %s\"" % (
                CONF.get("conf", "run_bin"), dst_db, proc["DST_USER_NAME"],
                des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                    base64.b64decode(proc["DST_PASSWORD"])).decode('utf-8', errors='ignore'),
                dst_user, dst_tab, where_value
            )
            LOGGER.info(tmp_cmd)
            ProcUtil().single_pro(tmp_cmd, get_rdbms_count, get_rdbms_count)
        elif proc["OPR_TYPE"] == 1 or proc["OPR_TYPE"] == "1":
            """全量导出"""
            tmp_cmd = "%s -e \"drop table if exists temp.%s_export_temp purge;create table temp.%s_export_temp as select %s from %s.%s\"" % (
                hive_bin, proc["SRC_TAB"], proc["SRC_TAB"], opr_columns, proc["SRC_USER"], proc["SRC_TAB"]
            )
            LOGGER.info(tmp_cmd)
            returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
                update_monitor(monitor_id, start, 0, True, True)
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            tmp_cmd = "%s eval --connect \"%s\" --username %s --password %s --verbose --query \"TRUNCATE TABLE %s.%s\"" % (
                CONF.get("conf", "run_bin"), dst_db, proc["DST_USER_NAME"],
                des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                    base64.b64decode(proc["DST_PASSWORD"])).decode('utf-8', errors='ignore'),
                dst_user, dst_tab
            )
            LOGGER.info(tmp_cmd)
            returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
                update_monitor(monitor_id, start, 0, True, True)
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            cmd = "%s export --username %s --password %s --verbose" % (
            CONF.get("conf", "run_bin"), proc["DST_USER_NAME"],
            des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                base64.b64decode(proc["DST_PASSWORD"])).decode('utf-8', errors='ignore'))

            cmd += " --connect \"%s\" %s --table %s --hcatalog-database temp --hcatalog-table %s_export_temp -m %s" % (
                dst_db, columns, out_tab, proc["SRC_TAB"], CONF.get("conf", "sqoop_num"))

            if map_column != "":
                cmd += " --map-column-java \"%s\"" % map_column
            cmd += " -- --default-character-set=utf-8 --delete-target-dir --direct"

            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
                update_monitor(monitor_id, start, 0, True, True)
            if succ_flag is False or is_stop() is True:
                fail(returncode)
                return

            tmp_cmd = "%s -e \"SELECT count(1) FROM temp.%s_export_temp\"" % (hive_bin, proc["SRC_TAB"])
            LOGGER.info(tmp_cmd)
            ProcUtil().single_pro(tmp_cmd, get_hive_count, get_hive_count)

            tmp_cmd = "%s eval --connect \"%s\" --username %s --password %s --verbose --query \"select count(1) from %s.%s\"" % (
                CONF.get("conf", "run_bin"), dst_db, proc["DST_USER_NAME"],
                des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
                    base64.b64decode(proc["DST_PASSWORD"])).decode('utf-8', errors='ignore'),
                dst_user, dst_tab
            )
            LOGGER.info(tmp_cmd)
            ProcUtil().single_pro(tmp_cmd, get_rdbms_count, get_rdbms_count)

        tmp_cmd = "%s -e \"drop table if exists temp.%s_export_temp purge;\"" % (hive_bin, proc["SRC_TAB"])
        LOGGER.info(tmp_cmd)
        returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
        if returncode != 0:
            succ_flag = False
            update_monitor(monitor_id, start, 0, True, True)
        if succ_flag is False:
            fail(returncode)
            return

    if succ_flag is True:
        succ()


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
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
            base64.b64decode(DB_CONF.get("db", "password"))),
        DB_CONF.get("db", "database"),
        DB_CONF.get("db", "port")
    )


def main():
    # kinit -kt hdfs.keytab hdfs/s101@HADOOP.COM
    # dingdian@DSJKAIFA.COM
    parallel_num = int(CONF.get("conf", "parallel_num"))
    pool = multiprocessing.Pool(processes=parallel_num)
    global GROUP, PUSH_STATUS_ID
    try:
        GROUP = get_sqoop_group(GROUP_ID)
        if GROUP is None:
            LOGGER.info("GROUP not exists")
            return

        LOGGER.info("GROUP: %s", GROUP)
        LOGGER.info("OPR_TYPE: %s", OPR_TYPE)
        procs = get_sqoop_proc(GROUP_ID)
        if OPR_TYPE == "2" or OPR_TYPE == 2:
            PUSH_STATUS_ID = push_status()
            LOGGER.info("PUSH_STATUS_ID: %s" % PUSH_STATUS_ID)
            if GROUP["IS_TS_LOG"] == "1" or GROUP["IS_TS_LOG"] == 1:
                push_other_status(PUSH_STATUS_ID)
        for proc in procs:
            pool.apply_async(execute_proc, args=(proc,))
    except Exception as e:
        LOGGER.info("error: %s" % e)
        end_dataflow_logs(3)
    pool.close()
    pool.join()
    if OPR_TYPE == "2" or OPR_TYPE == 2:
        update_push_status(PUSH_STATUS_ID)
        if GROUP["IS_TS_LOG"] == "1" or GROUP["IS_TS_LOG"] == 1:
            update_push_other_status(PUSH_STATUS_ID)


if __name__ == '__main__':
    read_config()
    init_param()
    main()
