#!/usr/bin/python
# coding=utf-8

"""
@name ODS源系统元数据采集器
@author chenss
@version 1.0.0
@update_time 2018-10-31
@comment 20191031 V1.0.0  chenshuangshui 新建
"""
import sys
import getopt
import datetime
import time
import configparser
import cx_Oracle
from utils.OdsDBUtil import  MysqlUtil
from utils.OracleUtil import OracleUtil
from utils.OdsDBUtil import SqlserverUtil
from utils.ProcUtil import ProcUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# reload(sys)
# 数据库配置文件
DB_CONF_PATH = sys.path[0] + "/conf/db.conf"
DB_CONF = None
# 配置文件
CONF_PATH = sys.path[0] + "/conf/Ods.conf"
CONF = None
MYSQL = None
MYSQL_HIVE = None
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
# 获取元数据类型（1代表采集，2代表推送）
FTYPE = 1

DBUSER = None
# 日志
LOGGER = None
LOG_FILE = None
# 主机
HOSTNODE = None


def show_help():
    """指令帮助"""
    print("""
    --SRCDB_ID            数据源（ALL表示全部）
    --DBUSER              数据库用户("ALL"表示全部）
    --FTYPE               获取元数据类型（1代表采集，2代表推送）
    """)
    sys.exit(1)


def validate_input():
    """验证参数"""
    if SRCDB_ID is None:
        print("please input --SRCDB_ID")
        LOGGER.info("please input --SRCDB_ID")
        sys.exit(1)


def init_param():
    """初始化参数"""
    # --SRCDB_ID=0 --DBUSER=ALL
    try:
        # 获取命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:", ["help", "SRCDB_ID=", "DBUSER=", "FTYPE="])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        show_help()
        sys.exit(1)

    for name, value in opts:
        if name in ("--SRCDB_ID",):
            global SRCDB_ID
            SRCDB_ID = value
        if name in ("--DBUSER",):
            global DBUSER
            DBUSER = value if value is not None else ""
            print(DBUSER)
        if name in ("--FTYPE",):
            global FTYPE
            FTYPE = value

    validate_input()


def read_config():
    # 读取配置文件
    print(sys.path[0])
    global DB_CONF, CONF, LOGGER, LOG_FILE
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    # 读取配置文件
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH, 'utf8')
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



def get_srcdb_list(id, ftype):
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
                WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user WHERE FTYPE=%s) AND (A.id=%s OR 'ALL'=%s)
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
            WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user WHERE FTYPE=%s) AND (A.SOURCE_NAME=%s OR 'ALL'=%s) 
            """
    res = MYSQL.query(sql, (ftype, id, id))
    return res


def get_srcdb_listold(id, ftype):
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
WHERE A.TYPE=B.IBM AND A.ID IN (SELECT SRC_DB FROM t_ods_srcdb_user WHERE FTYPE=%s) AND (A.SOURCE_NAME=%s OR 'ALL'=%s) 
"""
    # print(sql%(ftype,id,id))
    res = MYSQL.query(sql, (ftype, id, id))
    return res


def get_srcdb_user_list(srcdb, dbuser, ftype):
    """根据id获取数据源信息对应的用户"""
    if dbuser == None or dbuser == 'ALL':
        sql = """
          SELECT DB_USER FROM t_ods_srcdb_user T WHERE T.SRC_DB=%s AND T.FTYPE=%s
          """
        # print("2---------->"+dbuser)
        res = MYSQL.query(sql, (srcdb, ftype))

    else:
        dbuser = "'" + dbuser.replace(",", "','") + "'"
        sql = """
          SELECT DB_USER FROM t_ods_srcdb_user T WHERE T.SRC_DB=%s AND T.DB_USER in (%s) AND T.FTYPE=%s
        """
        # print("3------------->"+sql % (srcdb,dbuser,ftype))
        exec_sql = sql % (srcdb, dbuser, ftype)
        res = MYSQL.query(exec_sql, {})
    return res


def get_oracle_tab_metadata(srcdb, dbuser, ftype):
    """功能：获取oracle数据库表和字段元数据"""
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
    src_db = srcdb["ID"]
    srcdb_users = get_srcdb_user_list(src_db, dbuser, ftype)
    print(srcdb_users)
    for srcdb_user in srcdb_users:
        db_user = srcdb_user["DB_USER"]
        print(db_user)
        # 1:获取元数据
        # 1.1:获取表元数据信息
        getTableSql = """
    	SELECT T.OWNER,T.TABLE_NAME,T.COMMENTS FROM ALL_TAB_COMMENTS T WHERE T.TABLE_TYPE='TABLE' AND T.TABLE_NAME NOT LIKE 'BIN%' AND T.OWNER =:db_user  """
        tableMetadata = ORACLE.query(getTableSql, {"db_user": db_user});
        for table in tableMetadata:
            # print("TABLE SOURCE info：" + table["OWNER"] + "." + table["TABLE_NAME"])
            LOGGER.info("TABLE SOURCE info：" + table["OWNER"] + "." + table["TABLE_NAME"])

        # 1.2:获取表字段元数据信息
        getTableColSql = """
    	SELECT T.OWNER,
    	T.TABLE_NAME,
    	T.COLUMN_ID,
    	T.COLUMN_NAME,
        D.COMMENTS,    	
    	DATA_TYPE,
    	DATA_LENGTH,
    	NVL(DATA_PRECISION,DATA_LENGTH) AS DATA_PRECISION,
    	DATA_SCALE,
    	DATA_TYPE ||
    	DECODE(T.DATA_TYPE,
    			'DATE',
    			'',
    			'CLOB',
    			'',
    			'BLOB',
    			'',
    			'BFILE',
    			'',
    			'FLOAT',
    			'',
    			'LONG RAW',
    			'',
    			'LONG',
    			'',
    			'RAW',
              '(' || TO_CHAR(DATA_LENGTH) || ')',
              (DECODE(SIGN(INSTR(DATA_TYPE, 'CHAR')),
                      1,
                      '(' || TO_CHAR(DATA_LENGTH) || ')',
                      (DECODE(SUBSTR(DATA_TYPE, 1, 9),
                              'TIMESTAMP',
                              '',
                              (DECODE(NVL(DATA_PRECISION, -1),
                                      -1,
                                      (DECODE(NVL(DATA_SCALE, -1),
                                              -1,
                                              '(' || TO_CHAR(DATA_LENGTH) || ')',
                                              '(' || TO_CHAR(DATA_LENGTH) || ',' ||
                                              TO_CHAR(DATA_SCALE) || ')')),
                                      ('(' || TO_CHAR(DATA_PRECISION) || ',' ||
                                      TO_CHAR(DATA_SCALE) || ')')))))))) AS DATA_TYPE_FULL
    FROM ALL_TAB_COLUMNS T, ALL_COL_COMMENTS D, ALL_TAB_COMMENTS E
    WHERE T.OWNER = D.OWNER
    AND T.OWNER = E.OWNER
    AND T.TABLE_NAME = D.TABLE_NAME
    AND T.COLUMN_NAME = D.COLUMN_NAME
    AND T.TABLE_NAME = E.TABLE_NAME
    AND T.TABLE_NAME NOT LIKE 'BIN%'
    AND T.OWNER = :db_user
    ORDER BY T.TABLE_NAME, T.COLUMN_ID
    	"""
        tableColMetadata = ORACLE.query(getTableColSql, {"db_user": db_user});
        # for tableCol in tableColMetadata:
        #   print(tableCol["OWNER"], tableCol["TABLE_NAME"], tableCol["COLUMN_NAME"], tableCol["DATA_TYPE_FULL"])

        # 2 生成元数据
        dealOdsMetadata(src_db, db_user, tableMetadata, tableColMetadata, ftype)

    ORACLE.close()

def get_mysql_tab_metadata(srcdb, dbuser, ftype):
    """功能：获取mysql数据库表和字段元数据"""
    host = srcdb["HOST_IP"]
    user = srcdb["SRC_USER_NAME"]
    password = des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(srcdb["SRC_PASSWORD"])).decode('utf-8', errors='ignore')
    database = srcdb["SRC_DATABASE_NAME"]
    port = srcdb["HOST_PORT"]
    src_db = srcdb["ID"]
    MYSQL_SRC = MysqlUtil(host, user, password, database, port)
    srcdb_users = get_srcdb_user_list(src_db, dbuser, ftype)
    print(srcdb_users)
    for srcdb_user in srcdb_users:
        db_user = srcdb_user["DB_USER"]
        print(db_user)
        # 1:获取元数据
        # 1.1:获取表元数据信息
        getTableSql = """
            SELECT
                T.TABLE_SCHEMA AS OWNER,
                T.TABLE_NAME,
                T.TABLE_COMMENT AS COMMENTS
            FROM
                information_schema. TABLES T
            WHERE
                T.TABLE_TYPE = 'BASE TABLE'
            AND T.TABLE_SCHEMA=%s ;
            """
        tableMetadata = MYSQL_SRC.query(getTableSql, (db_user))
        for table in tableMetadata:
            LOGGER.info("TABLE SOURCE info：" + table["OWNER"] + "." + table["TABLE_NAME"])
            print("TABLE SOURCE info：" + table["OWNER"] + "." + table["TABLE_NAME"])

        # 1.2:获取表字段元数据信息
        getTableColSql = """
            SELECT
                T.TABLE_SCHEMA AS OWNER,
                T.TABLE_NAME,
                T.ORDINAL_POSITION AS COLUMN_ID,
                T.COLUMN_NAME,
                T.COLUMN_COMMENT AS COMMENTS,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH AS DATA_LENGTH,
                -- 以字符为单位的字段长度，CHARACTER_OCTET_LENGTH是以字节为单位的字段长度
                IFNULL(
                    NUMERIC_PRECISION,
                    CHARACTER_MAXIMUM_LENGTH
                ) AS DATA_PRECISION,
                NUMERIC_SCALE AS DATA_SCALE,
                COLUMN_TYPE AS DATA_TYPE_FULL
            FROM
                information_schema.COLUMNS T
            WHERE T.TABLE_SCHEMA=%s
            ORDER BY
                T.TABLE_NAME,
                T.ORDINAL_POSITION
    	"""
        tableColMetadata = MYSQL_SRC.query(getTableColSql, (db_user));
        # for tableCol in tableColMetadata:
        #   print(tableCol["OWNER"], tableCol["TABLE_NAME"], tableCol["COLUMN_NAME"], tableCol["DATA_TYPE_FULL"])
        # 2 生成元数据
        dealOdsMetadata(src_db, db_user, tableMetadata, tableColMetadata, ftype)
    MYSQL_SRC.close()

def get_sqlserver_tab_metadata(srcdb, dbuser, ftype):
    """功能：获取sqlserver数据库表和字段元数据"""
    host = srcdb["HOST_IP"]
    user = srcdb["SRC_USER_NAME"]
    password = des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(base64.b64decode(srcdb["SRC_PASSWORD"])).decode('utf-8', errors='ignore')
    database = srcdb["SRC_DATABASE_NAME"]
    port = srcdb["HOST_PORT"]
    src_db = srcdb["ID"]
    SQLSERVER_SRC = SqlserverUtil(host, user, password, database, port)
    srcdb_users = get_srcdb_user_list(src_db, dbuser, ftype)
    print(srcdb_users)
    for srcdb_user in srcdb_users:
        db_user = srcdb_user["DB_USER"]
        print(db_user)
        # 1:获取元数据
        # 1.1:获取表元数据信息
        getTableSql = """SELECT T.TABLE_SCHEMA AS OWNER,T.TABLE_NAME,'' AS COMMENTS FROM INFORMATION_SCHEMA.TABLES T WHERE T.TABLE_SCHEMA = %s; """
        tableMetadata = SQLSERVER_SRC.query(getTableSql, (db_user))
        for table in tableMetadata:
            LOGGER.info("TABLE SOURCE info：" + table["OWNER"] + "." + table["TABLE_NAME"])
            print("TABLE SOURCE info：" + table["OWNER"] + "." + table["TABLE_NAME"])

        # 1.2:获取表字段元数据信息
        getTableColSql = """
                SELECT
                    T.TABLE_SCHEMA AS OWNER,
                    T.TABLE_NAME,
                    T.ORDINAL_POSITION AS COLUMN_ID,
                    T.COLUMN_NAME,
                    '' AS COMMENTS,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH AS DATA_LENGTH,
                    -- 以字符为单位的字段长度，CHARACTER_OCTET_LENGTH是以字节为单位的字段长度
                    NUMERIC_PRECISION AS DATA_PRECISION ,
                    NUMERIC_SCALE AS DATA_SCALE,
                    CASE 
                        WHEN
                            DATA_TYPE='char' or DATA_TYPE='varchar' or DATA_TYPE='nvarchar' or DATA_TYPE='nchar'
                        THEN 
                            DATA_TYPE+'('+CAST(CHARACTER_MAXIMUM_LENGTH AS varchar)+')'
                        WHEN
                            DATA_TYPE='numeric' or DATA_TYPE='decimal'
                        THEN
                            DATA_TYPE+'('+CAST(NUMERIC_PRECISION AS varchar)+','+CAST(NUMERIC_SCALE AS varchar)+')'
                        ELSE
                            DATA_TYPE
                 END AS DATA_TYPE_FULL
                FROM
                    INFORMATION_SCHEMA.COLUMNS T
                WHERE T.TABLE_SCHEMA=%s
                ORDER BY
                    T.TABLE_NAME,
                    T.ORDINAL_POSITION
    	"""
        tableColMetadata = SQLSERVER_SRC.query(getTableColSql, (db_user));
        for tableCol in tableColMetadata:
            print(tableCol["OWNER"], tableCol["TABLE_NAME"], tableCol["COLUMN_NAME"], tableCol["DATA_TYPE_FULL"])
        # 2 生成元数据
        dealOdsMetadata(src_db, db_user, tableMetadata, tableColMetadata, ftype)


def get_bacth_id():
    """获取变更记录表的批次ID"""
    sql = """select bd.nextval('t_tables_chglog_batchid') as bacth_id from dual"""
    res = MYSQL.query(sql, ())
    return res[0]['bacth_id']


def dealOdsMetadata(src_db, db_user, tableMetadata, tableColMetadata, ftype):
    """功能：将获取的元数据信息写入MYSQL配置库中"""
    # 2:生成元数据变更信息
    # 2.1:将表信息数据生成到临时表中
    # 清空临时表
    # print(src_db,db_user)
    MYSQL.execute_sql("delete from pub_sys.t_ods_srcdb_tbl_last where src_db=%s and db_user=%s and ftype=%s",
                      (src_db, db_user, ftype));
    tabMetaList = []
    dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # print('1------------->'+dt)
    for tab in tableMetadata:
        # print(tab['TABLE_NAME'],tab['COMMENTS'],tab['OWNER'],src_db,dt,ftype)
        param = (tab['TABLE_NAME'], tab['COMMENTS'], tab['OWNER'], src_db, dt, ftype)
        tabMetaList.append(param)
    # 数据插入临时表
    insertTableSql_tmp = """
        	insert into pub_sys.t_ods_srcdb_tbl_last(TABLE_NAME,TABLE_COMMENT,DB_USER,SRC_DB,CREATE_TIME,FTYPE)
        	values (%s,%s,%s,%s,%s,%s)
        	"""
    # print(insertTableSql_tmp)
    # print(tabMetaList)
    if len(tabMetaList) != 0:
        MYSQL.executemany(insertTableSql_tmp, tabMetaList);

    # 2.2:将字段信息数据生成到临时表中
    # 清空临时表
    MYSQL.execute_sql("delete from pub_sys.t_ods_srcdb_tblcol_last where src_db=%s and db_user=%s and ftype=%s",
                      (src_db, db_user, ftype));
    tabColMetaList = []
    for tabCol in tableColMetadata:
        param = (tabCol['OWNER'], src_db, tabCol['TABLE_NAME'], tabCol['COLUMN_NAME'], tabCol['DATA_TYPE_FULL'],
                 tabCol['COMMENTS'], tabCol['DATA_PRECISION'], tabCol['DATA_SCALE'], tabCol['COLUMN_ID'], ftype)
        tabColMetaList.append(param)
    # 数据插入临时表
    insertTableSql_tmp = """
            insert into pub_sys.t_ods_srcdb_tblcol_last(DB_USER,SRC_DB,TABLE_NAME,COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT,COLUMN_LENGTH,COLUMN_ACCURACY,COLUMN_ID,ftype)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
    if len(tabColMetaList) != 0:
        MYSQL.executemany(insertTableSql_tmp, tabColMetaList);

    # 2.3:将表变更信息（新增表、删除表）写入表变更日志表
    sql = """
            INSERT INTO pub_sys.t_ods_srcdb_tbl_chglog(SRC_DB,DB_USER,TABLE_NAME,TABLE_COMMENT,CHANGE_LOG,CHANGE_TIME,batch_id, ftype)
    SELECT T.SRC_DB,T.DB_USER,T.TABLE_NAME,TABLE_COMMENT,T.CHG_TYPE,NOW() AS CREATE_TIME,%s AS BATCH_ID,%s AS FTYPE
    FROM (
    SELECT A.SRC_DB,A.DB_USER,A.TABLE_NAME,A.TABLE_COMMENT,'新增表' AS CHG_TYPE
      FROM pub_sys.t_ods_srcdb_tbl_last A
     WHERE (A.SRC_DB, A.DB_USER, A.TABLE_NAME) NOT IN
           (SELECT B.SRC_DB, B.DB_USER, B.TABLE_NAME FROM pub_sys.t_ods_srcdb_tbl B where B.FTYPe=%s)
       AND src_db=%s and db_user=%s and A.ftype=%s
    UNION ALL
    SELECT A.SRC_DB,A.DB_USER,A.TABLE_NAME,A.TABLE_COMMENT,'删除表' AS CHG_TYPE
      FROM pub_sys.t_ods_srcdb_tbl A
     WHERE (A.SRC_DB, A.DB_USER, A.TABLE_NAME) NOT IN
           (SELECT B.SRC_DB, B.DB_USER, B.TABLE_NAME FROM pub_sys.t_ods_srcdb_tbl_last B)
      AND src_db=%s and db_user=%s and A.ftype=%s
      ) T
            """
    # print(sql % (batch_id, ftype, ftype, src_db, db_user, ftype, src_db, db_user, ftype))
    MYSQL.execute_sql(sql, (batch_id, ftype, ftype, src_db, db_user, ftype, src_db, db_user, ftype));

    # 2.4:将表字段变更信息写入字段变更日志表
    sql = """
      INSERT INTO T_ODS_SRCDB_TBLCOL_CHGLOG
        (DB_USER, SRC_DB, TABLE_NAME, COLUMN_NAME,COLUMN_ID,COLUMN_COMMENT,COLUMN_TYPE, CHANGE_TYPE, CHANGE_COMMENT,COLUMN_LENGTH,COLUMN_ACCURACY,CREATE_TIME,batch_id,ftype)
        SELECT DB_USER, SRC_DB, TABLE_NAME, COLUMN_NAME,COLUMN_ID,COLUMN_COMMENT,COLUMN_TYPE, CHANGE_TYPE, CHANGE_COMMENT,COLUMN_LENGTH,COLUMN_ACCURACY, NOW() AS CREATE_TIME,%s AS batch_id,%s as ftype
          FROM (SELECT A.DB_USER,
                       A.SRC_DB,
                       A.TABLE_NAME,
                       A.COLUMN_ID,
                       A.COLUMN_NAME,
                       A.COLUMN_TYPE,
                       A.COLUMN_LENGTH,
                       A.COLUMN_ACCURACY,
                       A.COLUMN_COMMENT,
                       '字段类型变更' AS CHANGE_TYPE,
                       CONCAT(B.COLUMN_TYPE, ' 变更为 ', A.COLUMN_TYPE) AS CHANGE_COMMENT
                  FROM t_ods_srcdb_tblcol_last A, t_ods_srcdb_tblcol B
                 WHERE A.SRC_DB = B.SRC_DB
                   AND A.DB_USER = B.DB_USER
                   AND A.TABLE_NAME = B.TABLE_NAME
                   AND A.COLUMN_NAME = B.COLUMN_NAME
                   AND A.COLUMN_TYPE <> B.COLUMN_TYPE
                   AND a.src_db=%s and a.db_user=%s and a.ftype=%s AND b.ftype=%s
                UNION ALL
                SELECT A.DB_USER,
                       A.SRC_DB,
                       A.TABLE_NAME,
                       A.COLUMN_ID,
                       A.COLUMN_NAME,
                       A.COLUMN_TYPE,
                       A.COLUMN_LENGTH,
                       A.COLUMN_ACCURACY,
                       A.COLUMN_COMMENT,
                       '字段新增' AS CHANGE_TYPE,
                       '' AS CHANGE_COMMENT
                  FROM t_ods_srcdb_tblcol_last A
                 WHERE NOT EXISTS (SELECT 1
                          FROM t_ods_srcdb_tblcol B
                         WHERE A.SRC_DB = B.SRC_DB
                           AND A.DB_USER = B.DB_USER
                           AND A.TABLE_NAME = B.TABLE_NAME
                           AND A.COLUMN_NAME = B.COLUMN_NAME AND B.ftype=%s)
                   AND a.src_db=%s and a.db_user=%s and a.ftype=%s
                UNION ALL
                SELECT A.DB_USER,
                       A.SRC_DB,
                       A.TABLE_NAME,
                       A.COLUMN_ID,
                       A.COLUMN_NAME,
                       A.COLUMN_TYPE,
                       A.COLUMN_LENGTH,
                       A.COLUMN_ACCURACY,
                       A.COLUMN_COMMENT,
                       '字段删除' AS CHANGE_TYPE,
                       '' AS CHANGE_COMMENT
                  FROM t_ods_srcdb_tblcol A
                 WHERE NOT EXISTS (SELECT 1
                          FROM t_ods_srcdb_tblcol_last B
                         WHERE A.SRC_DB = B.SRC_DB
                           AND A.DB_USER = B.DB_USER
                           AND A.TABLE_NAME = B.TABLE_NAME
                           AND A.COLUMN_NAME = B.COLUMN_NAME AND B.FTYPE=%s)
                   AND a.src_db=%s and a.db_user=%s and a.ftype=%s
                   ) T
                    """
    MYSQL.execute_sql(sql, (
        batch_id, ftype, src_db, db_user, ftype, ftype, ftype, src_db, db_user, ftype, ftype, src_db, db_user,
        ftype));

    # 2.4:将表变更信息（字段变更）写入表变更日志表
    sql = """
            INSERT INTO pub_sys.t_ods_srcdb_tbl_chglog
    (SRC_DB, DB_USER, TABLE_NAME, TABLE_COMMENT, CHANGE_LOG, CHANGE_TIME, BATCH_ID,ftype)
    SELECT A.SRC_DB,
           A.DB_USER,
           A.TABLE_NAME,
           '' AS TABLE_COMMENT,
           '字段变更' AS CHG_TYPE,
           NOW() AS CREATE_TIME,
           A.BATCH_ID,
           %s AS ftype
      FROM t_ods_srcdb_tblcol_chglog A
     WHERE NOT EXISTS (SELECT 1
              FROM t_ods_srcdb_tbl_chglog B
             WHERE A.SRC_DB = B.SRC_DB
               AND A.DB_USER = B.DB_USER
               AND A.TABLE_NAME = B.TABLE_NAME
               AND A.BATCH_ID = B.BATCH_ID)
       AND A.BATCH_ID=%s
       AND A.SRC_DB=%s 
       AND A.DB_USER=%s AND A.FTYPE=%s
     GROUP BY A.SRC_DB, A.DB_USER, A.TABLE_NAME, A.BATCH_ID
            """
    MYSQL.execute_sql(sql, (ftype, batch_id, src_db, db_user, ftype));

    # 3:生成最终元数据
    # 3.1:生成表元数据
    sql = """
                INSERT INTO pub_sys.t_ods_srcdb_tbl(SRC_DB,DB_USER,TABLE_NAME,TABLE_COMMENT,CREATE_TIME,ftype)
                SELECT SRC_DB,DB_USER,TABLE_NAME,TABLE_COMMENT,CHANGE_TIME,%s AS ftype
                 FROM pub_sys.t_ods_srcdb_tbl_chglog t 
                WHERE  (SRC_DB, DB_USER, TABLE_NAME) NOT IN
                    (SELECT B.SRC_DB, B.DB_USER, B.TABLE_NAME FROM pub_sys.t_ods_srcdb_tbl B where ftype=%s) 
                  AND t.batch_id=%s AND t.CHANGE_LOG='新增表' AND src_db=%s and db_user=%s and ftype=%s
                """
    # print(sql % (ftype, ftype, batch_id, src_db, db_user, ftype))
    MYSQL.execute_sql(sql, (ftype, ftype, batch_id, src_db, db_user, ftype));

    sql = """
                DELETE FROM  pub_sys.t_ods_srcdb_tbl
                 WHERE (SRC_DB, DB_USER, TABLE_NAME) IN (
                      SELECT SRC_DB, DB_USER, TABLE_NAME
                      FROM pub_sys.t_ods_srcdb_tbl_chglog t 
                      WHERE t.batch_id=%s AND t.CHANGE_LOG='删除表' AND t.src_db=%s and t.db_user=%s and t.ftype=%s)
                  AND src_db=%s and db_user=%s and ftype=%s                     
                """
    MYSQL.execute_sql(sql, (batch_id, src_db, db_user, ftype, src_db, db_user, ftype));

    # 3.2:生成表字段元数据
    sql = """
                INSERT INTO t_ods_srcdb_tblcol
        (DB_USER,
         SRC_DB,
         TABLE_NAME,
         COLUMN_ID,
         COLUMN_NAME,
         COLUMN_TYPE,
         COLUMN_LENGTH,
         COLUMN_ACCURACY,
         COLUMN_COMMENT,ftype)
        SELECT DB_USER,
               SRC_DB,
               TABLE_NAME,
               COLUMN_ID,
               COLUMN_NAME,
               COLUMN_TYPE,
               COLUMN_LENGTH,
               COLUMN_ACCURACY,
               COLUMN_COMMENT,
               %s as ftype
          FROM pub_sys.t_ods_srcdb_tblcol_chglog A
         WHERE NOT EXISTS (SELECT 1
                  FROM t_ods_srcdb_tblcol B
                 WHERE A.SRC_DB = B.SRC_DB
                   AND A.DB_USER = B.DB_USER
                   AND A.TABLE_NAME = B.TABLE_NAME
                   AND A.COLUMN_NAME = B.COLUMN_NAME
                   AND A.COLUMN_TYPE = B.COLUMN_TYPE AND B.FTYPE=%s)
           AND A.CHANGE_TYPE = '字段新增'
           AND A.batch_id=%s AND src_db=%s and db_user=%s AND A.ftype=%s
                """
    MYSQL.execute_sql(sql, (ftype, ftype, batch_id, src_db, db_user, ftype));

    sql = """
            DELETE FROM t_ods_srcdb_tblcol
               WHERE (SRC_DB,DB_USER,TABLE_NAME,COLUMN_NAME) IN 
                    (SELECT SRC_DB,DB_USER,TABLE_NAME,COLUMN_NAME
                       FROM pub_sys.t_ods_srcdb_tblcol_chglog A
                      WHERE A.batch_id=%s
                        AND A.CHANGE_TYPE = '字段删除'
                        AND src_db=%s and db_user=%s and A.ftype=%s)
                 AND src_db=%s and db_user=%s AND ftype=%s
                """
    MYSQL.execute_sql(sql, (batch_id, src_db, db_user, ftype, src_db, db_user, ftype));

    sql = """
            UPDATE t_ods_srcdb_tblcol A,t_ods_srcdb_tblcol_chglog B
				SET A.COLUMN_TYPE=B.COLUMN_TYPE,A.COLUMN_ACCURACY=B.COLUMN_ACCURACY,A.COLUMN_LENGTH=B.COLUMN_LENGTH
                 WHERE A.SRC_DB = B.SRC_DB
                   AND A.DB_USER = B.DB_USER
                   AND A.TABLE_NAME = B.TABLE_NAME
                   AND A.COLUMN_NAME = B.COLUMN_NAME
				   AND B.CHANGE_TYPE = '字段类型变更'
                   AND B.batch_id=%s AND A.src_db=%s and A.db_user=%s
                   AND A.ftype=%s AND B.ftype=%s
                """
    # print(sql%(batch_id, src_db, db_user,ftype,ftype))
    MYSQL.execute_sql(sql, (batch_id, src_db, db_user, ftype, ftype));


def main():
    global batch_id
    global batch_id_hive
    batch_id = get_bacth_id()
    batch_id_hive = batch_id
    print("SRC_ID:"+SRCDB_ID)
    srcdbs = get_srcdb_list(SRCDB_ID, FTYPE)
    # print(batch_id)
    for srcdb in srcdbs:
        print("1------------->" + srcdb["SOURCE_NAME"])
        # print(DBUSER)
        LOGGER.info("DB SOURCE info：" + srcdb["SOURCE_NAME"])
        if srcdb["DATASORCE_TYPE"] == 'ORACLE':
            get_oracle_tab_metadata(srcdb, DBUSER, FTYPE)
        if srcdb["DATASORCE_TYPE"] == 'MYSQL':
            get_mysql_tab_metadata(srcdb, DBUSER, FTYPE)
        if srcdb["DATASORCE_TYPE"] == 'SQLSERVER':
            get_sqlserver_tab_metadata(srcdb, DBUSER, FTYPE)
    # 关闭连接
    MYSQL.close()


if __name__ == '__main__':
    read_config()
    init_param()
    main()
