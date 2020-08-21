#!/usr/bin/python
# coding=utf-8

"""
@name 元数据管理的源系统元数据采集器
@author chenss
@version 1.0.0
@update_time 2018-10-31
@comment 20191031 V1.0.0  chenshuangshui 新建
"""
import datetime
import sys
import getopt
import time
import configparser
from utils.DBUtil import MysqlUtil
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
CONF_PATH = sys.path[0] + "/conf/RunMetadataCollect.conf"
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
# 获取元数据类型（1代表采集，2代表推送，3代表hive）
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
        opts, args = getopt.getopt(sys.argv[1:], "ht:v:l:", ["help", "SRCDB_ID=", "DBUSER="])
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

    validate_input()

def read_config():
    # 读取配置文件
    print(sys.path[0])
    global DB_CONF, CONF, LOGGER, LOG_FILE,HIVE_DB_NAME
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    # 读取配置文件
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH, 'utf8')
    LOG_FILE = (sys.path[0] + "/" + CONF.get("conf", "log_file")) % time.strftime("%Y%m%d", time.localtime())
    #现场环境为空
    HIVE_DB_NAME=CONF.get("conf","hive_db")
    if HIVE_DB_NAME.strip()=='':
        HIVE_DB_NAME="metastore"
    HIVE_DB_NAME=HIVE_DB_NAME+"."
    print(HIVE_DB_NAME)
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
    # 连接hive元数据库
    global MYSQL_HIVE
    MYSQL_HIVE = MysqlUtil(
        DB_CONF.get("hive_db", "host"),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
            base64.b64decode(DB_CONF.get("hive_db", "user"))),
        des(key=DB_CONF.get("conf", "des_key"), padmode=PAD_PKCS5).decrypt(
            base64.b64decode(DB_CONF.get("hive_db", "password"))),
        DB_CONF.get("hive_db", "database"),
        DB_CONF.get("hive_db", "port")
    )
    # mysql5.7.x版本中，sql_mode的only_full_group_by模式是默认（开启），导致有些sql不兼容，临时改下模式
    MYSQL_HIVE.execute_sql("set session sql_mode='';", ());

def get_srcdb_list(id):
    """根据id获取数据源信息"""
    res = None
    sql = """
         SELECT DB_NAME, 'HIVE' AS SOURCE_NAME,'HIVE' AS  DATASORCE_TYPE FROM pub_sys.t_databases d WHERE d.DB_NAME = %s or 'ALL' = %s;
        """
    res = MYSQL.query(sql, (id, id))
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

def get_bacth_id():
    """获取变更记录表的批次ID"""
    sql = """select bd.nextval('t_tables_chglog_batchid') as bacth_id from dual"""
    res = MYSQL.query(sql, ())
    return res[0]['bacth_id']

def get_hive_tab_metadata(db_name):
    # 获取pub_sys.t_databases下的ID作为DB_ID
    res = None
    res = MYSQL.query("select id as DB_ID from t_databases a where a.DB_NAME='%s';" % db_name, ());
    print("select id as DB_ID from t_databases a where a.DB_NAME='%s';" % db_name, ())
    for r in res:
        DB_ID = r["DB_ID"]
    print("batch_id_hive的值为："+str(batch_id_hive))
    # 获取表信息
    sql = """
    SELECT
        A.TBL_NAME AS TBL_NAME,
        A.TBL_TYPE AS TBL_TYPE,
        %s AS DB_ID,  -- 此DB_ID不是元数据的DB_ID，是配置表pub_sys.t_databases下的ID
        B.`NAME` AS DB_NAME,
        D.PARAM_VALUE AS TBL_COMMENT,
        from_unixtime(A.CREATE_TIME) AS CREATE_TIME,
        CASE
    WHEN P.TBL_ID IS NOT NULL THEN
        1
    ELSE
        0
    END AS IS_PART_TAB,
     P.PART_COLUMNS AS PART_COLUMNS,
     CURRENT_TIMESTAMP AS UPDATE_TIME
    FROM
        %sTBLS A -- 表 
    JOIN %sDBS B -- 库
    ON A.DB_ID = B.DB_ID -- JOIN pub_sys.t_databases C -- 已配置数据库
    -- ON B. NAME = C.DB_NAME -- 限定数据库或者test.T_HIVE_DBS表中所有的库
    AND (B. NAME = '%s')
    LEFT JOIN %sTABLE_PARAMS D -- 表/视图的属性信息
    ON A.TBL_ID = D.TBL_ID
    AND D.PARAM_KEY = 'comment'
    LEFT JOIN (
        SELECT
            TBL_ID,
            GROUP_CONCAT(
                PKEY_NAME,
                '(',
                PKEY_TYPE,
                ')'
            ) AS PART_COLUMNS
        FROM
            %sPARTITION_KEYS -- 分区的字段信息
        GROUP BY
            TBL_ID
    ) P ON A.TBL_ID = P.TBL_ID
    ORDER BY
        A.TBL_NAME;
""" % (DB_ID,HIVE_DB_NAME,HIVE_DB_NAME,db_name,HIVE_DB_NAME,HIVE_DB_NAME)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"--->获取表信息")
    print("1----------------------------->"+sql)

    res = MYSQL_HIVE.query(sql, ());
    if None == res:
        print('该数据库没有没有相关表')
        return
    # 插入前删除原有记录
    MYSQL.execute_sql("DELETE FROM pub_sys.t_tables_last WHERE DB_NAME = '%s';" % db_name, ());
    # 插入数据前先删除原来的记录
    MYSQL.execute_sql("DELETE FROM pub_sys.t_tab_columns_last WHERE DB_NAME = '%s';" % db_name, ());
    for tab in res:
        insert_sql = """INSERT INTO pub_sys.t_tables_last (TBL_NAME,TBL_TYPE,DB_ID,DB_NAME,TBL_COMMENT,CREATE_TIME,IS_PART_TAB,PART_COLUMNS,UPDATE_TIME) 
           values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        # print(str(tuple(tab.values())))
        MYSQL.execute_sql(insert_sql, (tab['TBL_NAME'], tab['TBL_TYPE'], tab['DB_ID'], tab['DB_NAME'], tab['TBL_COMMENT'], tab['CREATE_TIME'], tab['IS_PART_TAB'], tab['PART_COLUMNS'], tab['UPDATE_TIME']));
        # 根据表名获取字段信息
        get_col_sql = """
        (
            SELECT
                D.`NAME` AS DB_NAME,
                T.TBL_NAME,
                C.INTEGER_IDX AS COLUMN_ID,
                C.COLUMN_NAME,
                C.TYPE_NAME AS COLUMN_TYPE,
                C.`COMMENT` AS COL_COMMENT,
                0 AS IS_PART_COLUMN
            FROM
                %sDBS D,
                -- 库
                %sTBLS T,
                -- 表
                %sSDS S,
                -- 存储
                %sCOLUMNS_V2 C -- 字段
            WHERE
                D.DB_ID = T.DB_ID
            AND T.SD_ID = S.SD_ID
            AND S.CD_ID = C.CD_ID
            AND CONVERT (
                unhex(
                    hex(
                        CONVERT (T.TBL_NAME USING latin1)
                    )
                ) USING utf8
            ) = '%s'
            AND CONVERT (
                unhex(
                    hex(
                        CONVERT (D.`NAME` USING latin1)
                    )
                ) USING utf8
            ) = '%s'
            ORDER BY C.INTEGER_IDX
        )
        UNION ALL
            (
                SELECT
                    D.`NAME` AS DB_NAME,
                    T.TBL_NAME,
                    MAX(C.INTEGER_IDX) + 1 AS COLUMN_ID,
                    P.PKEY_NAME AS COLUMN_NAME,
                    P.PKEY_TYPE AS COLUMN_TYPE,
                    '分区字段' AS COL_COMMENT,
                    1 AS IS_PART_COLUMN
                FROM
                    %sDBS D,
                    -- 库
                    %sTBLS T,
                    -- 表
                    %sSDS S,
                    %sPARTITION_KEYS P,
                    -- 分区字段
                    %sCOLUMNS_V2 C -- 字段
                WHERE
                    D.DB_ID = T.DB_ID
                AND T.TBL_ID = P.TBL_ID
                AND T.SD_ID = S.SD_ID
                AND S.CD_ID = C.CD_ID
                AND CONVERT (
                    unhex(
                        hex(
                            CONVERT (T.TBL_NAME USING latin1)
                        )
                    ) USING utf8
                ) = '%s'
                AND CONVERT (
                    unhex(
                        hex(
                            CONVERT (D.`NAME` USING latin1)
                        )
                    ) USING utf8
                ) = '%s'
            );""" % (HIVE_DB_NAME,HIVE_DB_NAME,HIVE_DB_NAME,HIVE_DB_NAME,tab["TBL_NAME"], db_name, HIVE_DB_NAME,HIVE_DB_NAME,HIVE_DB_NAME,HIVE_DB_NAME,HIVE_DB_NAME,tab["TBL_NAME"], db_name)
        # print(get_col_sql)
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->根据表名获取字段信息")
        print("2----------------------------->" + get_col_sql)
        col_res = MYSQL_HIVE.query(get_col_sql, ())
        for col in col_res:
            # print(str(tuple(col.values())))
            if(col['COLUMN_NAME'] !=None):
                """ 没有分区字段的表会关联到空，过滤"""
                print(col['DB_NAME']+'\t'+col['TBL_NAME']+'\t'+col['COLUMN_NAME']+'\t'+str(col['COLUMN_ID']))
                insert_sql = """INSERT INTO pub_sys.t_tab_columns_last (DB_NAME,TBL_NAME,COLUMN_ID,COLUMN_NAME,COLUMN_TYPE,COL_COMMENT,IS_PART_COLUMN) 
                   values(%s,%s,%s,%s,%s,%s,%s)"""
                MYSQL.execute_sql(insert_sql, (col['DB_NAME'], col['TBL_NAME'], col['COLUMN_ID'], col['COLUMN_NAME'], col['COLUMN_TYPE'], col['COL_COMMENT'], col['IS_PART_COLUMN']))
    # 记录表、字段变更日志

    # 1将表变更信息写入表变更日志表
    sql = """
    INSERT INTO pub_sys.t_tables_chglog (
        DB_ID,
        DB_NAME,
        TABLE_NAME,
        TABLE_COMMENT,
        CHANGE_LOG,
        CHANGE_TIME,
        BATCH_ID
    ) SELECT
        T.DB_ID,
        T.DB_NAME,
        T.TBL_NAME,
        T.TBL_COMMENT,
        T.CHANGE_LOG,
        NOW() AS CREATE_TIME ,
        %s AS BATCH_ID
    FROM
        (
            SELECT
                A.DB_ID,
                A.DB_NAME,
                A.TBL_NAME,
                A.TBL_COMMENT,
                '新增表' AS CHANGE_LOG
            FROM
                pub_sys.t_tables_last A
            WHERE
                (
                    A.DB_ID,
                    A.DB_NAME,
                    A.TBL_NAME
                ) NOT IN (
                    SELECT
                        B.DB_ID,
                        B.DB_NAME,
                        B.TBL_NAME
                    FROM
                        pub_sys.t_tables B
                )
            AND DB_NAME = '%s'
            UNION ALL
                SELECT
                    A.DB_ID,
                    A.DB_NAME,
                    A.TBL_NAME,
                    A.TBL_COMMENT,
                    '删除表' AS CHANGE_LOG
                FROM
                    pub_sys.t_tables A
                WHERE
                    (
                        A.DB_NAME,
                        A.TBL_NAME
                    ) NOT IN (
                        SELECT
                            B.DB_NAME,
                            B.TBL_NAME
                        FROM
                            pub_sys.t_tables_last B
                    )
                AND DB_NAME = '%s'
        ) T
            """ % (batch_id_hive,db_name,db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("3---------------------------->" + sql)
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->将表变更信息写入表变更日志表")
    MYSQL.execute_sql(sql, ());


    # 2将表字段变更信息写入字段变更日志表
    sql = """
    INSERT INTO pub_sys.t_tab_columns_chglog (
        DB_ID,
        DB_NAME,
        TABLE_NAME,
        CHANGE_TYPE,
        CHANGE_COMMENT,
        COLUMN_NAME,
        COLUMN_COMMENT,
        COLUMN_TYPE,
        COLUMN_ID,
        BATCH_ID,
        CREATE_TIME
    ) SELECT
        DB_ID,
        DB_NAME,
        TBL_NAME,
        CHANGE_TYPE,
        CHANGE_COMMENT,
        COLUMN_NAME,
        COL_COMMENT,
        COLUMN_TYPE,
        COLUMN_ID,
        %s AS BATCH_ID,
        NOW() AS CREATE_TIME
    FROM
        (
            SELECT
                %s AS DB_ID,
                A.DB_NAME,
                A.TBL_NAME,
                A.COLUMN_ID,
                A.COLUMN_NAME,
                A.COLUMN_TYPE,
                A.COL_COMMENT,
                '字段类型变更' AS CHANGE_TYPE,
                CONCAT(
                    B.COLUMN_TYPE,
                    ' 变更为 ',
                    A.COLUMN_TYPE
                ) AS CHANGE_COMMENT
            FROM
                t_tab_columns_last A,
                t_tab_columns B
            WHERE
                A.DB_NAME = B.DB_NAME
            AND A.TBL_NAME = B.TBL_NAME
            AND A.COLUMN_NAME = B.COLUMN_NAME
            AND A.COLUMN_TYPE <> B.COLUMN_TYPE
            AND A.DB_NAME ='%s'
            UNION ALL
                SELECT
                    %s AS DB_ID,
                    A.DB_NAME,
                    A.TBL_NAME,
                    A.COLUMN_ID,
                    A.COLUMN_NAME,
                    A.COLUMN_TYPE,
                    A.COL_COMMENT,
                    '字段新增' AS CHANGE_TYPE,
                    '' AS CHANGE_COMMENT
                FROM
                    t_tab_columns_last A
                WHERE
                    NOT EXISTS (
                        SELECT
                            1
                        FROM
                            t_tab_columns B
                        WHERE
                            A.DB_NAME = B.DB_NAME
                        AND A.TBL_NAME = B.TBL_NAME
                        AND A.COLUMN_NAME = B.COLUMN_NAME
                    )
                AND a.DB_NAME ='%s'
                UNION ALL
                    SELECT
                    %s AS DB_ID,
                    A.DB_NAME,
                    A.TBL_NAME,
                    A.COLUMN_ID,
                    A.COLUMN_NAME,
                    A.COLUMN_TYPE,
                    A.COL_COMMENT,
                        '字段删除' AS CHANGE_TYPE,
                        '' AS CHANGE_COMMENT
                    FROM
                        t_tab_columns A
                    WHERE
                        NOT EXISTS (
                            SELECT
                                1
                            FROM
                                t_tab_columns_last B
                            WHERE
                                A.DB_NAME = B.DB_NAME
                            AND A.TBL_NAME = B.TBL_NAME
                            AND A.COLUMN_NAME = B.COLUMN_NAME
                        )
                    AND a.DB_NAME ='%s'
        ) T
                    """ % (batch_id_hive, DB_ID, db_name, DB_ID, db_name, DB_ID, db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->将表字段变更信息写入字段变更日志表")
    print("4----------------------------->" + sql)
    MYSQL.execute_sql(sql, ());

    # 3将表变更信息（字段变更）写入表变更日志表
    sql = """
    INSERT INTO pub_sys.t_tables_chglog (
        DB_ID,
        DB_NAME,
        TABLE_NAME,
        TABLE_COMMENT,
        CHANGE_LOG,
        CHANGE_TIME,
        BATCH_ID
    ) SELECT
        A.DB_ID,
        A.DB_NAME,
        A.TABLE_NAME,
        '' AS TABLE_COMMENT,
        '字段变更' AS CHANGE_LOG,
        NOW() AS CHANGE_TIME,
        A.BATCH_ID AS BATCH_ID
    FROM
        t_tab_columns_chglog A
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM
                t_tables_chglog B
            WHERE
                A.DB_NAME = B.DB_NAME
            AND A.TABLE_NAME = B.TABLE_NAME
            AND A.BATCH_ID = B.BATCH_ID
        )
    AND A.BATCH_ID=%s
    AND A.DB_NAME = '%s'
    GROUP BY
        A.DB_NAME,
        A.TABLE_NAME,
        A.BATCH_ID
            """ % (batch_id_hive, db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("5----------------------------->" + sql)
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->将表变更信息（字段变更）写入表变更日志表")
    MYSQL.execute_sql(sql, ());

    # 4写入信息表
    sql = """
    INSERT INTO pub_sys.t_tables (
        TBL_NAME,
        TBL_TYPE,
        DB_ID,
        DB_NAME,
        TBL_COMMENT,
        CREATE_TIME,
        IS_PART_TAB,
        PART_COLUMNS,
        UPDATE_TIME
    ) SELECT
        C.TBL_NAME,
        C.TBL_TYPE,
        C.DB_ID,
        C.DB_NAME,
        C.TBL_COMMENT,
        C.CREATE_TIME,
        C.IS_PART_TAB,
        C.PART_COLUMNS,
        NOW() AS UPDATE_TIME
    FROM
        pub_sys.t_tables_chglog T, pub_sys.t_tables_last C
    WHERE
        (T.DB_NAME, T.TABLE_NAME) NOT IN (
            SELECT
                B.DB_NAME,
                B.TBL_NAME AS TABLE_NAME
            FROM
                pub_sys.t_tables B
        )
    AND T.CHANGE_LOG = '新增表'
    AND T.BATCH_ID = %s
    AND T.DB_NAME = C.DB_NAME
    AND T.TABLE_NAME = C.TBL_NAME
    AND T.DB_NAME = '%s'
                """ % (batch_id_hive, db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->写入信息表")
    print("6----------------------------->" + sql)
    MYSQL.execute_sql(sql, ());

    sql = """
    DELETE
    FROM
        pub_sys.t_tables
    WHERE
        (DB_NAME, TBL_NAME) IN (
            SELECT
                DB_NAME,
                TABLE_NAME AS TBL_NAME
            FROM
                pub_sys.t_tables_chglog t
            WHERE t.CHANGE_LOG = '删除表'
            AND t.BATCH_ID = %s
            AND t.DB_NAME = '%s'
        )
    AND DB_NAME = '%s'                     
                """ % (batch_id_hive, db_name, db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->写入信息表 2")
    print("7----------------------------->" + sql)
    MYSQL.execute_sql(sql, ());

    # 5入字段信息表
    sql = """
     INSERT INTO pub_sys.t_tab_columns (
        DB_NAME,
        TBL_NAME,
        COLUMN_ID,
        COLUMN_NAME,
        COLUMN_TYPE,
        COL_COMMENT,
        IS_PART_COLUMN,
        CODE_VALUE,
        REMARK,
        IS_SENSITIVE,
        ENCRYPTION
    ) SELECT
        C.DB_NAME,
        C.TBL_NAME,
        C.COLUMN_ID,
        C.COLUMN_NAME,
        C.COLUMN_TYPE,
        C.COL_COMMENT,
        C.IS_PART_COLUMN,
        C.CODE_VALUE,
        C.REMARK,
        C.IS_SENSITIVE,
        C.ENCRYPTION
    FROM
        pub_sys.t_tab_columns_chglog A,pub_sys.t_tab_columns_last C
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM
                t_tab_columns B
            WHERE
                A.DB_NAME = B.DB_NAME
            AND A.TABLE_NAME = B.TBL_NAME
            AND A.COLUMN_NAME = B.COLUMN_NAME
            AND A.COLUMN_TYPE = B.COLUMN_TYPE
        )
    AND A.CHANGE_TYPE = '字段新增'
    AND A.DB_NAME = C.DB_NAME
    AND A.BATCH_ID = %s
    AND A.TABLE_NAME = C.TBL_NAME
    AND A.COLUMN_NAME = C.COLUMN_NAME
    AND A.DB_NAME = '%s'
                """ % (batch_id_hive, db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->写入字段信息表")
    print("8----------------------------->" + sql)
    MYSQL.execute_sql(sql, ());

    sql = """
    DELETE
    FROM
        t_tab_columns
    WHERE
        (
            DB_NAME,
            TBL_NAME,
            COLUMN_NAME
        ) IN (
            SELECT
                DB_NAME,
                TABLE_NAME AS TBL_NAME,
                COLUMN_NAME
            FROM
                pub_sys.t_tab_columns_chglog A
            WHERE
            A.BATCH_ID = %s
            AND A.CHANGE_TYPE = '字段删除'
            AND DB_NAME = '%s'
        )
    AND DB_NAME = '%s'
                """ % (batch_id_hive, db_name, db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->写入字段信息表 2")
    print("9----------------------------->" + sql)
    MYSQL.execute_sql(sql, ());

    sql = """
    UPDATE t_tab_columns A,
     t_tab_columns_chglog B
    SET A.COLUMN_TYPE = B.COLUMN_TYPE
    WHERE
        A.DB_NAME = B.DB_NAME
    AND A.TBL_NAME = B.TABLE_NAME
    AND A.COLUMN_NAME = B.COLUMN_NAME
    AND B.CHANGE_TYPE = '字段类型变更'
    AND B.BATCH_ID = %s
    AND A.DB_NAME = '%s'
                """ % (batch_id_hive, db_name)
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("10----------------------------->" + sql)
    LOGGER.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "--->写入字段信息表 3")
    MYSQL.execute_sql(sql, ());
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def main():
    global batch_id
    global batch_id_hive
    batch_id = get_bacth_id()
    batch_id_hive = batch_id
    print("SRC_ID:"+SRCDB_ID)
    srcdbs = get_srcdb_list(SRCDB_ID)
    print(batch_id)
    for srcdb in srcdbs:
        print("1------------->" + srcdb["SOURCE_NAME"])
        # print(DBUSER)
        LOGGER.info("DB SOURCE info：" + srcdb["SOURCE_NAME"])
        if srcdb["DATASORCE_TYPE"] == 'HIVE':
            try:
                get_hive_tab_metadata(srcdb["DB_NAME"])
            except Exception as e:
                print(str(e))
                LOGGER.error("get_hive_tab_metadata() err：" + str(e))
                exit(1)
    # 关闭连接
    # MYSQL.close()
    # MYSQL_HIVE.close()

if __name__ == '__main__':
    read_config()
    init_param()
    main()
