#!/usr/bin/python
# coding=utf-8
"""
@name ODS建表脚本生成程序
@author chenss
@version 1.0.0
@update_time 2018-11-06
@comment 20181106 V1.0.0  chenshuangshui 新建
"""
import sys
import getopt
import datetime
import time
import configparser
from utils.OdsDBUtil import  MysqlUtil
from utils.OracleUtil import OracleUtil
from utils.ProcUtil import ProcUtil
from utils.LogUtil import Logger
from utils.OSUtil import OSUtil

from pyDes import des, PAD_PKCS5
import base64

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
# Sqoop分组ID
GROUP_ID = None
# 日志
LOGGER = None
LOG_FILE = None
# 主机
HOSTNODE = None

# 数据源ID
SRCDB_ID = None
# 数据库用户
DBUSER = None
# 表名称
TBLNAME = None


def show_help():
    """指令帮助"""
    print("""
    --SRCDB_ID            数据源（ALL表示全部）
    --DBUSER              数据库用户("ALL"表示全部）
    --TBLNAME             数据表（"ALL"表示全部）     
    --GENFILEMODE         生成文件模式（1表示同一个系统的建表语句在一个文件；2|表示同一个系统同一个用户的建表语句在一个文件；3|表示一张表一个文件）         
    """)
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
    if TBLNAME is None:
        print("please input --GENFILEMODE")
        LOGGER.info("please input --GENFILEMODE")
        sys.exit(1)


def init_param():
    """初始化参数"""
    # python OdsGenCreateTblSql.py --SRCDB_ID=33 --DBUSER=ALL --TBLNAME=ALL --GENFILEMODE=3
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:",
                                   ["help", "SRCDB_ID=", "DBUSER=", "TBLNAME=", "GENFILEMODE="])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        show_help()
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
        if name in ("--GENFILEMODE",):
            global GENFILEMODE
            GENFILEMODE = value
    validate_input()


def read_config():
    # 读取配置文件
    global DB_CONF, CONF, LOGGER, LOG_FILE, CREATETBL_DIR, STORAGE_TYPE, HDFS_LOCATION
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
            WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user where ftype='1') AND (A.id=%s OR 'ALL'=%s)
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
        WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user where ftype='1') AND (A.SOURCE_NAME=%s OR 'ALL'=%s) 
        """
    res = MYSQL.query(sql, (id, id))
    return res


def get_srcdb_listold(id):
    """根据id获取数据源信息"""
    res = None
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
WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user) AND (A.SOURCE_NAME=%s OR 'ALL'=%s) 
"""
    res = MYSQL.query(sql, (id, id))
    return res


def get_srdb_tbl_list(srcdb, dbuser, tblname):
    """获取对应表的信息"""
    """若指定表名为TSO_KHXX这种带有下划线的，程序认为‘_’不是字母也不是数字
     isalpha()为false。导致sql语句结果为空，"""
    if tblname[0].isalpha():
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
            AND (D.ID IN (%s) OR 'ALL' IN (%s))
            AND A.FTYPE='1' AND D.FTYPE='1'
            """
    exec_sql = sql % (srcdb, srcdb, dbuser, dbuser, tblname, tblname)
    # print(exec_sql)
    res = MYSQL.query(exec_sql, {})
    return res


def get_srdb_tbl_listold(srcdb, dbuser, tblname):
    """获取对应表的信息"""

    dbuser = "'" + dbuser.replace(",", "','") + "'"
    tblname = "'" + tblname.replace(",", "','") + "'"

    sql = """
SELECT A.SRC_DB, C.TYPE, A.DB_USER, A.TABLE_NAME,
        C.TYPE AS SRC_DB_TYPE,
        concat('ods','_',lower(C.SOURCE_NAME)) AS ODS_DB_NAME,
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
    res = MYSQL.query(exec_sql, {})
    return res


def get_srdb_tbl_listold(srcdb, dbuser, tblname):
    """获取对应表的信息"""

    sql = """
SELECT A.SRC_DB, C.TYPE, A.DB_USER, A.TABLE_NAME,
       C.TYPE AS SRC_DB_TYPE,
       concat('ods','_',lower(C.SOURCE_NAME)) AS ODS_DB_NAME,
       concat(lower(A.DB_USER),'_',lower(A.TABLE_NAME)) AS ODS_TBL_NAME,
       d.COLLECT_STRATEGY,d.INCR_COLNAME,d.INCR_TIMEDIM, d.DT_MODE
  FROM t_ods_srcdb_tbl A, t_srm_datasource C,t_ods_srcdb_collect_manage D
 WHERE A.SRC_DB = C.ID
   AND D.STATUS=1
   AND A.SRC_DB=D.SRC_DB 
   AND A.DB_USER=D.DB_USER 
   AND A.TABLE_NAME=D.TABLE_NAME
   AND (A.SRC_DB = %s OR 0=%s)
   AND (A.DB_USER = %s OR 'ALL'=%s)
   AND (A.TABLE_NAME = %s OR 'ALL'=%s)
   AND D.FTYPE='1'
"""
    res = MYSQL.query(sql, (srcdb, srcdb, dbuser, dbuser, tblname, tblname))
    return res


def get_srdb_tblcol(srcdb, dbuser, tblname):
    """获取对应表的列信息"""
    sql = """
    SELECT A.SRC_DB,
       C.TYPE AS SRC_DB_TYPE,
       A.DB_USER,
       A.TABLE_NAME,
       A.TABLE_COMMENT,
       B.COLUMN_NAME,
       B.COLUMN_COMMENT,
       CASE 
            WHEN ISNULL(B.COLUMN_TYPECHG) OR (COLUMN_TYPECHG='')  THEN 
                B.COLUMN_TYPE
            ELSE
                B.COLUMN_TYPECHG
            END  AS COLUMN_TYPE,
       B.COLUMN_ID
  FROM t_ods_srcdb_tbl A, t_ods_srcdb_tblcol B, t_srm_datasource C
 WHERE A.SRC_DB = B.SRC_DB
   AND A.DB_USER = B.DB_USER
   AND A.TABLE_NAME = B.TABLE_NAME
   AND A.SRC_DB = C.ID
   AND A.SRC_DB = %s
   AND A.DB_USER = %s
   AND A.TABLE_NAME = %s
   AND A.FTYPE='1'
   AND B.FTYPE='1'
 ORDER BY COLUMN_ID
"""
    res = MYSQL.query(sql, (srcdb, dbuser, tblname))
    return res


def tran_type(src_db_type, src_data_type):
    """
    类型转换器
    :param src_data_type: 源库的字段定义
    :return: Hive字段定义
    """
    dst_data_type = ""

    sql = """
        SELECT SRC_DATATYPE,DST_DATATYPE FROM t_ods_srcdb_datatype_mapping t WHERE t.DB_TYPE=%s
    """
    type_map_list = MYSQL.query(sql, (src_db_type))
    for _type in type_map_list:
        # print(src_data_type,_type["SRC_DATATYPE"],_type["DST_DATATYPE"],src_data_type.split('(')[0])
        if src_data_type.split('(')[0].upper() == _type["SRC_DATATYPE"].upper():
            dst_data_type = src_data_type.replace(_type["SRC_DATATYPE"], _type["DST_DATATYPE"])
            # 小写的也做转换
            dst_data_type = dst_data_type.replace(_type["SRC_DATATYPE"].lower(), _type["DST_DATATYPE"])
            # print(src_data_type+'->' + _type["DST_DATATYPE"]+"->" + dst_data_type)
            if _type["DST_DATATYPE"].upper() == 'VARCHAR' or _type["DST_DATATYPE"].upper() == 'VARCHAR2':
                #  print(src_data_type, _type["SRC_DATATYPE"], _type["DST_DATATYPE"], src_data_type,int(src_data_type.split('(')[1].split(')')[0])*2)
                dst_data_type = _type["DST_DATATYPE"] + "(" + str(
                    int(src_data_type.split('(')[1].split(')')[0]) * 2) + ")"
            if _type["SRC_DATATYPE"].upper() == 'NUMBER' and len(src_data_type.split(',')) > 1 and _type["DST_DATATYPE"].upper() =='DECIMAL':
                #  number(2,4)的形式，number(p,s)，如果 p<s ，则 number(p,s)转化为decimal(s+2,s)
                if(int(src_data_type.split('(')[1].split(')')[0].split(',')[0]) < int(
                    src_data_type.split('(')[1].split(')')[0].split(',')[1])):
                    dst_data_type = "DECIMAL(" + str(
                        int(src_data_type.split('(')[1].split(')')[0].split(',')[1]) + 2) + ',' + str(
                        int(src_data_type.split('(')[1].split(')')[0].split(',')[1])) + ")"

            if _type["SRC_DATATYPE"].upper() == 'NUMBER' and len(src_data_type.split(',')) > 1 and _type["DST_DATATYPE"].upper() =='DECIMAL':
                # number(5,-3)形式，number(p,s)，如果 s<0 ，则 number(p,s)转化为decimal,不指定精度
                if(int(src_data_type.split('(')[1].split(')')[0].split(',')[1]) < 0):
                    dst_data_type = "DECIMAL"

            # 如果目标数据类型不是DECIMAL或者VARCHAR或者CHAR，则按照‘(’切分，取切分后的第一个值
            if _type["DST_DATATYPE"].upper() != 'DECIMAL' and _type["DST_DATATYPE"].upper() != 'VARCHAR' and _type["DST_DATATYPE"].upper() != 'CHAR':
                dst_data_type = dst_data_type.split('(')[0]
            # print(src_data_type + '->' + _type["DST_DATATYPE"] + "->" + dst_data_type)
            # int(10) unsigned 形式的数据类型过滤掉unsigned
            dst_data_type = dst_data_type.replace('unsigned', '')

    return dst_data_type


def get_sql_str(column_name, column_type, column_comment, table_comment, is_last_field):
    """
    格式化字段语句
    :param column_name: 字段名
    :param column_type: 字段类型
    :param column_comment: 字段注释
    :param table_comment: 表注释
    :param is_last_field: 是否末尾字段
    :param is_stage: 是否贴源层
    :return: 格式化语句
    """
    sql_str = "    `" + column_name.lower() + "` " + column_type + (
        " COMMENT '" + column_comment + "'" if str(column_comment) != "None" else "")
    if is_last_field:
        sql_str += "\n)" + "\n";
        # sql_str += "\n)" + "\n" + (" COMMENT '" + str(table_comment) + "'" if str(column_comment) != "None" else "")
    else:
        sql_str += "," + "\n"
    return sql_str


def gen_createtbl_sql(tbl, is_stage):
    """
    生成建表脚本
    :param tbl: 表信息
    :param is_stage: 是否贴源层
    """

    ods_db_type = tbl["TYPE"]
    ods_db = tbl["ODS_DB_NAME"]
    # 分区模式：0|不分区；1|按日分区；2|按月分区；3|按年分区,默认根据日分区
    dt_mode = tbl['DT_MODE']
    if is_stage == 1:
        ods_db = 'stage_' + ods_db;
        ods_tbl = tbl["ODS_TBL_NAME"]
    else:
        ods_tbl = tbl["ODS_TBL_NAME"]
        if dt_mode == 0:
            partition_str = ''
        else:
            partition_str = "PARTITIONED BY ( dt STRING )"
        storage_str = "STORED AS " + STORAGE_TYPE

    cols = get_srdb_tblcol(tbl["SRC_DB"], tbl["DB_USER"], tbl["TABLE_NAME"])
    column_id_max = len(cols)
    # 表名根据元数据表信息获取，很可能会存在表名，没有字段的情况，在此判断，如果没有字段直接返回空，避免生成的建表语句错误
    if column_id_max < 1:
        return ""
    # 生成建表脚本
    create_sql = ""
    for col in cols:
        # 判断是否最后一列:
        is_last_field = (column_id_max == col["COLUMN_ID"])
        dst_col_type = tran_type(ods_db_type, col["COLUMN_TYPE"])
        col_name = col["COLUMN_NAME"]
        col_comment = str(col["COLUMN_COMMENT"]).replace("'", "").replace(";", "")
        tab_comment = col["TABLE_COMMENT"]
        # print(col_name,ods_db_type,dst_col_type)
        if col["COLUMN_ID"] == 1:
            # create_sql += "DROP TABLE IF NOT EXISTS " + ods_db + "." + ods_tbl + ";\n"
            create_sql += "CREATE EXTERNAL TABLE  IF NOT EXISTS " + ods_db + "." + ods_tbl + " (" + "\n"
            create_sql += get_sql_str(col_name, dst_col_type, col_comment, tab_comment, is_last_field)
        # print(get_sql_str(col["COLUMN_NAME"], col["COLUMN_TYPE"], col["COLUMN_COMMENT"], col["TABLE_COMMENT"],column_id_max))
        # print(col["COLUMN_NAME"], col["COLUMN_TYPE"], col["COLUMN_COMMENT"], col["TABLE_COMMENT"],column_id_max)
        else:
            create_sql += get_sql_str(col_name, dst_col_type, col_comment, tab_comment, is_last_field)

    # 贴源层表和目标层表存储方式不同：
    if is_stage == 0:
        create_sql += (partition_str + "\n") if partition_str != '' else partition_str
        create_sql += storage_str + "\n"
    create_sql += "LOCATION " + "'" + HDFS_LOCATION + "/" + ods_db + ".db/" + ods_tbl + "'" + ";\n"
    # print(create_sql)
    return create_sql


def gen_createdb_sql(ods_db):
    """
    生成目标层建库语句
    :param ods_db:需要创建的数据库
    :return:
    """
    create_sql = ""
    # create_sql += "DROP DATABASE IF NOT EXISTS " + ods_db + " CASCADE;\n"
    create_sql += "CREATE DATABASE  IF NOT EXISTS " + ods_db + "\n"
    create_sql += "LOCATION " + "'" + HDFS_LOCATION + "/" + ods_db + ".db" + "';\n"
    # print(create_sql)
    return create_sql


def gen_hdfs_sh(ods_db, ods_tbl, is_stage):
    """
    生成创建HDFS目录的sh脚本
    :param ods_db:数据库
    :param ods_tbl:数据表
    :return:
    """
    create_hdfs = ""
    if is_stage == 0:
        create_hdfs += "hdfs dfs -mkdir -p " + HDFS_LOCATION + "/" + ods_db + ".db/" + ods_tbl + "\n"
    else:
        create_hdfs += "hdfs dfs -mkdir -p " + HDFS_LOCATION + "/" + "stage_" + ods_db + ".db/" + ods_tbl + "\n"
    return create_hdfs


def main():
    list = []  ## 空列表
    # 创建目录
    OSUtil.mkdir(CREATETBL_DIR)
    srcdbs = get_srcdb_list(SRCDB_ID);
    stage_file_name = ""
    ods_file_name = ""
    # 创建对接各个系统的ODS库
    if srcdbs == []:
        raise Exception("库不存在")
    for srcdb in srcdbs:
        ods_createtbl_dir = CREATETBL_DIR + srcdb["ODS_DB_NAME"]
        OSUtil.mkdir(ods_createtbl_dir)
        # print(ods_createtbl_dir)
        # 创建HDFS目录的文件名
        hdfs_file_name = ods_createtbl_dir + "/" + "hdfs_" + srcdb["ODS_DB_NAME"] + ".sh"
        stage_hdfs_file_name = ods_createtbl_dir + "/" + "stage_hdfs_" + srcdb["ODS_DB_NAME"] + ".sh"

        tbls = get_srdb_tbl_list(srcdb['ID'], DBUSER, TBLNAME)
        runRows = 0
        if tbls == []:
            raise Exception("表不存在")
        for tbl in tbls:
            # 生成文件模式（1表示同一个系统的建表语句在一个文件；2 | 表示同一个系统同一个用户的建表语句在一个文件；3 | 表示一张表一个文件）
            if GENFILEMODE == "1":
                stage_file_name = ods_createtbl_dir + "/" + "stage_" + srcdb["ODS_DB_NAME"] + ".sql"
                ods_file_name = ods_createtbl_dir + "/" + srcdb["ODS_DB_NAME"] + ".sql"
            if GENFILEMODE == "2":
                stage_file_name = ods_createtbl_dir + "/" + "stage_" + srcdb["ODS_DB_NAME"] + "_" + tbl[
                    "DB_USER"] + ".sql"
                ods_file_name = ods_createtbl_dir + "/" + srcdb["ODS_DB_NAME"] + "_" + tbl["DB_USER"] + ".sql"
            if GENFILEMODE == "3":
                stage_file_name = ods_createtbl_dir + "/" + "stage_" + srcdb["ODS_DB_NAME"] + "_" + tbl[
                    "ODS_TBL_NAME"] + ".sql "
                ods_file_name = ods_createtbl_dir + "/" + srcdb["ODS_DB_NAME"] + "_" + tbl["ODS_TBL_NAME"] + ".sql"
            runRows += 1
            # 新增创建建库文件，无论是否存在表 --缓冲层
            createdb_sql = gen_createdb_sql("stage_" + tbl['ODS_DB_NAME'])
            db_file_name = ods_createtbl_dir + "/" + "stage_db_" + srcdb["ODS_DB_NAME"] + ".sql"
            if runRows == 1:
                list.append(tbl['ODS_DB_NAME'])
                OSUtil.file_save(db_file_name, createdb_sql)
            else:
                if tbl['ODS_DB_NAME'] not in list:
                    OSUtil.file_append(db_file_name, createdb_sql)

            # 创建建库文件，无论是否存在表--目标层
            createdb_sql = gen_createdb_sql(tbl['ODS_DB_NAME'])
            db_file_name = ods_createtbl_dir + "/" + "db_" + srcdb["ODS_DB_NAME"] + ".sql"
            if runRows == 1:
                list.append(tbl['ODS_DB_NAME'])
                OSUtil.file_save(db_file_name, createdb_sql)
            else:
                if tbl['ODS_DB_NAME'] not in list:
                    list.append(tbl['ODS_DB_NAME'])
                    OSUtil.file_append(db_file_name, createdb_sql)
            # print(runRows)
            # print(tbl["SRC_DB"],tbl["TYPE"],tbl["DB_USER"],tbl["TABLE_NAME"])
            # 生成贴源层表建表语句
            stage_create_sql = gen_createtbl_sql(tbl, 1)
            # 生成ODS层表建表语句
            ods_create_sql = gen_createtbl_sql(tbl, 0)
            # 生成贴源层创建HDFS目录语句
            stage_hdfs_sh = gen_hdfs_sh(tbl["ODS_DB_NAME"], tbl['ODS_TBL_NAME'], 1)
            # 生成ODS层创建HDFS目录语句
            hdfs_sh = gen_hdfs_sh(tbl["ODS_DB_NAME"], tbl['ODS_TBL_NAME'], 0)

            # 写入文本文件，首次写入重写文件，后续追加写入文件
            if runRows == 1:
                sFirstline = "#! /bin/sh\n";
                OSUtil.file_save(stage_hdfs_file_name, sFirstline + stage_hdfs_sh)
                OSUtil.file_save(hdfs_file_name, sFirstline + hdfs_sh)
                OSUtil.file_save(stage_file_name, stage_create_sql)
                OSUtil.file_save(ods_file_name, ods_create_sql)
            else:
                OSUtil.file_append(stage_hdfs_file_name, stage_hdfs_sh)
                OSUtil.file_append(hdfs_file_name, hdfs_sh)
                OSUtil.file_append(stage_file_name, stage_create_sql)
                OSUtil.file_append(ods_file_name, ods_create_sql)

    # 关闭连接
    MYSQL.close()


if __name__ == '__main__':
    read_config()
    init_param()
    main()
