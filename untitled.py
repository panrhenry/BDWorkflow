#!/usr/bin/env python
# encoding: utf-8
# 如果觉得不错，可以推荐给你的朋友！http://tool.lu/pyc
'''
@name sqoop\xe6\x89\xa7\xe8\xa1\x8c\xe5\x99\xa8
@author jiangbing
@version 1.0.0
@update_time 2018-06-25
@comment 20180625 V1.0.0  jiangbing \xe6\x96\xb0\xe5\xbb\xba
'''
import sys
import getopt
import datetime
import time
import configparser
from utils.DBUtil import MysqlUtil
from utils.ProcUtil import ProcUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64
import multiprocessing
DB_CONF_PATH = sys.path[0] + '/conf/db.conf'
DB_CONF = None
CONF_PATH = sys.path[0] + '/conf/RunSqoop.conf'
CONF = None
MYSQL = None
LSH = None
VAR = { }
TYPE = None
OPR_TYPE = None
GROUP_ID = None
LOGGER = None
LOG_FILE = None
IMPALA_HOSTNODE = None
HIVE_HOSTNODE = None

def show_help():
    '''\xe6\x8c\x87\xe4\xbb\xa4\xe5\xb8\xae\xe5\x8a\xa9'''
    print('\n    -t                    \xe8\xb0\x83\xe5\xba\xa6\xe7\xb1\xbb\xe5\x9e\x8b\n    -v                    \xe4\xb8\xaa\xe6\x80\xa7\xe5\x8f\x82\xe6\x95\xb0\n    -l                    \xe6\xb5\x81\xe6\xb0\xb4\xe5\x8f\xb7\n    --opr_type            \xe6\x93\x8d\xe4\xbd\x9c\xe7\xb1\xbb\xe5\x9e\x8b\n    --group_id            Sqoop\xe5\x88\x86\xe7\xbb\x84ID\n    ')
    sys.exit()


def validate_input():
    '''\xe9\xaa\x8c\xe8\xaf\x81\xe5\x8f\x82\xe6\x95\xb0'''
    if TYPE is None:
        print('please input -t')
        LOGGER.info('please input -t')
        sys.exit(1)
    if LSH is None:
        print('please input -l')
        LOGGER.info('please input -l')
        sys.exit(1)
    if OPR_TYPE is None:
        print('please input --opr_type')
        LOGGER.info('please input --opr_type')
        sys.exit(1)
    if GROUP_ID is None:
        print('please input --group_id')
        LOGGER.info('please input --group_id')
        sys.exit(1)


def init_param():
    '''\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe5\x8f\x82\xe6\x95\xb0'''
    global TYPE, LSH, OPR_TYPE, GROUP_ID
    
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'ht:v:l:', [
            'help',
            'type=',
            'var=',
            'lsh=',
            'opr_type=',
            'group_id='])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        None
        None
        None
        show_help()
        sys.exit(1)

    for (name, value) in opts:
        if name in ('-h', '--help'):
            show_help()
        if name in ('-t', '--type'):
            TYPE = value
        if name in ('-v', '--var') and value != '':
            tmp = value.split('&')
            for item in tmp:
                t = item.split('=')
                if t[1] is not None and t[1] != '':
                    VAR[t[0]] = t[1]
        if name in ('-l', '--lsh'):
            LSH = value
        if name in ('--opr_type',):
            OPR_TYPE = value
        if name in ('--group_id',):
            GROUP_ID = value
    validate_input()


def get_sqoop_proc(id):
    '''\xe6\xa0\xb9\xe6\x8d\xaeid\xe8\x8e\xb7\xe5\x8f\x96\xe6\x95\xb0\xe6\x8d\xae\xe5\xa4\x84\xe7\x90\x86\xe7\xa8\x8b\xe5\xba\x8f'''
    sql = """
        SELECT a.SRC_USER,a.DST_USER,a.C_TABLE,a.P_KEY,a.TYPE_TAB,a.OPR_DIMENSION,a.COL_DIMENSION,a.OPR_COLUMN,a.OPR_TYPE,
          b.DB_LINK AS SRC_DB,b.DATABASE_NAME AS SRC_DATABASE_NAME,b.TYPE AS SRC_TYPE,b.USER_NAME AS SRC_USER_NAME,
          b.PASSWORD AS SRC_PASSWORD,IFNULL(c.TABLE_NAME,a.SRC_TAB) AS SRC_TAB,IFNULL(c.TABLE_NAME_CN,a.SRC_TAB) AS SRC_TAB_CN,
          d.DB_LINK AS DST_DB,d.DATABASE_NAME AS DST_DATABASE_NAME,d.TYPE AS DST_TYPE,d.USER_NAME AS DST_USER_NAME,
          d.PASSWORD AS DST_PASSWORD,IFNULL(e.TABLE_NAME,a.DST_TAB) AS DST_TAB,IFNULL(e.TABLE_NAME_CN,a.DST_TAB) AS DST_TAB_CN
          FROM t_job_sqoop_proc a
          LEFT JOIN t_srm_datasource b ON a.SRC_DB_ID=b.ID
          LEFT JOIN t_table c ON (a.SRC_TAB=c.TABLE_NAME AND a.SRC_USER=c.OWNER)
          LEFT JOIN t_srm_datasource d ON a.DST_DB_ID=d.ID
          LEFT JOIN t_table e ON (a.DST_TAB=e.TABLE_NAME AND a.DST_USER=e.OWNER)
          WHERE a.GROUP_ID=%s
    """
    if 'PROC' in VAR:
        sql += ' AND a.ID IN (%s)' % VAR['PROC']
    res = MYSQL.query(sql, (id,))
    return res


def get_impala_hostnode():
    '''\xe8\x8e\xb7\xe5\x8f\x96impala\xe4\xb8\xbb\xe6\x9c\xba'''
    sql = '\n        SELECT HOST_IP,USER_NAME,PASSWORD,DB_LINK FROM t_srm_datasource WHERE TYPE=5 LIMIT 1\n    '
    res = MYSQL.query(sql, ())
    LOGGER.info('get_hostnode: %s' % res)
    if len(res) > 0:
        return res[0]
    return None


def get_hive_hostnode():
    '''\xe8\x8e\xb7\xe5\x8f\x96impala\xe4\xb8\xbb\xe6\x9c\xba'''
    sql = '\n        SELECT HOST_IP,USER_NAME,PASSWORD,DB_LINK FROM t_srm_datasource WHERE TYPE=2 LIMIT 1\n    '
    res = MYSQL.query(sql, ())
    LOGGER.info('get_hostnode: %s' % res)
    if len(res) > 0:
        return res[0]
    return None


def start_proc_logs(PROC_NAME, PROC_DESC):
    '''\xe8\xae\xb0\xe5\xbd\x95\xe7\xa8\x8b\xe5\xba\x8f\xe6\x97\xa5\xe5\xbf\x97'''
    sql = '\n    insert into t_etl_proc_log(\n' \
          '    `DETAIL_LOG_ID`,' \
          '\n    `PROC_DESC`,' \
          '\n    `PROC_NAME`,\n' \
          '    `STAT_DATE`,\n  ' \
          '  `START_TIME`,\n ' \
          '  `STATUS`,\n  ' \
          '  `RUN_TYPE`\n    )' \
          'values(%s,%s,%s,%s,%s,%s,%s)\n    '
    rq = 0
    if 'RQ' in VAR:
        rq = VAR['RQ']
    elif 'rq' in VAR:
        rq = VAR['rq']
    id = MYSQL.execute_sql(sql, (LSH, PROC_DESC, PROC_NAME, rq, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 1, TYPE))
    return id


def end_proc_logs(ID, start, STATUS):
    '''\xe4\xbf\xae\xe6\x94\xb9\xe7\xa8\x8b\xe5\xba\x8f\xe6\x97\xa5\xe5\xbf\x97'''
    COST_TIME = int(time.time()) - start
    sql = 'UPDATE t_etl_proc_log SET END_TIME=%s,COST_TIME=%s,STATUS=%s WHERE ID=%s'
    LOGGER.info(sql % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), COST_TIME, STATUS, ID))
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), COST_TIME, STATUS, ID))
    if STATUS == 3 or STATUS == 4:
        end_dataflow_logs(STATUS)
    else:
        end_dataflow_logs()


def update_proc_logs(ID, PROCESS_ID):
    '''\xe4\xbf\xae\xe6\x94\xb9\xe7\xa8\x8b\xe5\xba\x8f\xe6\x97\xa5\xe5\xbf\x97'''
    sql = 'UPDATE t_etl_proc_log SET PROCESS_ID=%s WHERE ID=%s'
    MYSQL.execute_sql(sql, (PROCESS_ID, ID))


def is_stop():
    '''\xe6\x98\xaf\xe5\x90\xa6\xe7\xbb\x88\xe6\xad\xa2\xe6\x89\xa7\xe8\xa1\x8c\xe7\xa8\x8b\xe5\xba\x8f'''
    sql = 'SELECT ID FROM t_etl_proc_log WHERE DETAIL_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=4'
    res = MYSQL.query(sql, (LSH, TYPE))
    return len(res) > 0


def end_dataflow_logs(STATUS = None):
    '''\xe4\xbf\xae\xe6\x94\xb9\xe8\x8a\x82\xe7\x82\xb9\xe6\x97\xa5\xe5\xbf\x97'''
    if STATUS is None:
        sql = 'UPDATE t_etl_dataflow_logs SET LOG_PATH=%s WHERE ID=%s'
        MYSQL.execute_sql(sql, (LOG_FILE, LSH))
    else:
        sql = 'UPDATE t_etl_dataflow_logs SET STATUS=%s, LOG_PATH=%s  WHERE ID=%s'
        MYSQL.execute_sql(sql, (STATUS, LOG_FILE, LSH))


def execute_proc(proc):
    '''sqoop\xe6\x89\xa7\xe8\xa1\x8c\xe5\x91\xbd\xe4\xbb\xa4'''
    global HIVE_HOSTNODE, IMPALA_HOSTNODE
    if is_stop() is True:
        return None
    id = None(proc['SRC_TAB'], proc['SRC_TAB_CN'])
    start = int(time.time())
    succ_flag = True
    rq = 0
    if 'RQ' in VAR:
        rq = VAR['RQ']
    elif 'rq' in VAR:
        rq = VAR['rq']
    where_value = ''
    if proc['OPR_DIMENSION'] == 1:
        where_value = rq[0:4]
    elif proc['OPR_DIMENSION'] == 2:
        where_value = rq[0:6]
    elif proc['OPR_DIMENSION'] == 3:
        where_value = rq
    
    def add_log(pid, returncode, outs, errs):
        LOGGER.info('exec_cmd id:%s,  pid:%s, returncode:%s' % (id, pid, returncode))
        LOGGER.info('exec_cmd outs: %s' % outs)
        LOGGER.info('exec_cmd errs: %s' % errs)
        if returncode == -9:
            end_proc_logs(id, start, 4)

    
    def succ():
        end_proc_logs(id, start, 2)

    
    def fail():
        end_proc_logs(id, start, 3)

    
    def before(pid):
        update_proc_logs(id, pid)

    cmd = ''
    HIVE_HOSTNODE = get_hive_hostnode()
    hive_bin = CONF.get('conf', 'hive_bin')
    if hive_bin.count('%s') == 1:
        hive_bin = hive_bin % HIVE_HOSTNODE['DB_LINK']
    elif hive_bin.count('%s') == 3:
        hive_bin = hive_bin % (HIVE_HOSTNODE['DB_LINK'], HIVE_HOSTNODE['USER_NAME'],
                               des(key = DB_CONF.get('conf', 'des_key'),
                            padmode = PAD_PKCS5).decrypt(base64.b64decode(HIVE_HOSTNODE['PASSWORD'])).decode('utf-8', errors = 'ignore'))
    impala_bin = hive_bin
    if CONF.get('conf', 'platform') == 'cdh':
        IMPALA_HOSTNODE = get_impala_hostnode()
        impala_bin = CONF.get('conf', 'impala_bin')
        if impala_bin.count('%s') == 1:
            impala_bin = impala_bin % IMPALA_HOSTNODE['HOST_IP']
        elif impala_bin.count('%s') == 2:
            impala_bin = impala_bin % (IMPALA_HOSTNODE['HOST_IP'], IMPALA_HOSTNODE['USER_NAME'])
        elif impala_bin.count('%s') == 3:
            impala_bin = impala_bin % (IMPALA_HOSTNODE['HOST_IP'], IMPALA_HOSTNODE['USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(IMPALA_HOSTNODE['PASSWORD'])).decode('utf-8', errors = 'ignore'))
    if OPR_TYPE == '1':
        src_db = proc['SRC_DB']
        src_user = proc['SRC_USER']
        src_tab = proc['SRC_TAB']
        in_tab = src_tab
        opr_column = '*'
        opr_columns = '*'
        columns = ''
        insert_column = ''
        if proc['SRC_TYPE'] == 1:
            src_db = src_db.replace(proc['SRC_DATABASE_NAME'], proc['SRC_USER'])
        if proc['SRC_TYPE'] == 4:
            src_user = str(src_user).upper()
            src_tab = str(src_tab).upper()
            -- in_tab = '%s.%s' % (src_user, src_tab)
			in_tab = '%s'% ( src_tab)+'.'+'%s' % ( src_tab)
        if proc['OPR_COLUMN'] is not None and proc['OPR_COLUMN'] != '':
            opr_column = proc['OPR_COLUMN']
            if proc['SRC_TYPE'] == 4:
                opr_column = str(opr_column).upper()
            columns = '--columns "%s"' % opr_column
            opr_columns = opr_column
            if proc['OPR_TYPE'] == 3:
                opr_columns = '%s,%s' % (opr_column, proc['COL_DIMENSION'])
                columns = '--columns "%s"' % opr_columns
        if opr_column != '*':
            insert_column = '(%s)' % opr_column
        tmp_cmd = '%s -e "drop table if exists temp.%s_import_temp purge;' \
                  'create table temp.%s_import_temp as select %s from %s.%s where 1=2;' \
                  'alter table temp.%s_import_temp set SERDEPROPERTIES(\'serialization.null.format\' = \'\\\\\\N\');"'\
                  % (hive_bin, proc['DST_TAB'], proc['DST_TAB'], opr_columns, proc['DST_USER'], proc['DST_TAB'], proc['DST_TAB'])
        LOGGER.info(tmp_cmd)
        returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
        if returncode != 0:
            succ_flag = False
        if is_stop() is True:
            return None
        if ((None, (None, (None, None))),)['OPR_TYPE'] == 2:
            cmd = '%s import --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['SRC_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['SRC_PASSWORD'])).decode('utf-8', errors = 'ignore'))
            cmd += ' --connect "%s" %s --table %s --null-string \'\\\\N\' --null-non-string \'\\\\N\' --hive-import ' \
                   '--hive-table temp.%s_import_temp --where "%s=%s" -m %s' \
                   % (src_db, columns, in_tab, proc['DST_TAB'], proc['COL_DIMENSION'], where_value, CONF.get('conf', 'sqoop_num'))
            cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if is_stop() is True:
                return None
            if None.get('conf', 'platform') == 'cdh':
                tmp_cmd = '%s -q "INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s SELECT * FROM (SELECT %s FROM temp.%s_import_temp UNION ALL SELECT %s FROM %s.%s WHERE %s<>%s) T"' % (impala_bin, proc['DST_TAB'], proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'], opr_column, proc['DST_USER'], proc['DST_TAB'], proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            if CONF.get('conf', 'platform') == 'tdh':
                tmp_cmd = '%s -e "INSERT OVERWRITE TABLE %s.%s%s SELECT * FROM (SELECT %s FROM temp.%s_import_temp UNION ALL SELECT %s FROM %s.%s WHERE %s<>%s) T"' % (impala_bin, proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'], opr_column, proc['DST_USER'], proc['DST_TAB'], proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            end_proc_logs(id, start, 3)
        elif proc['OPR_TYPE'] == 3:
            cmd = '%s import --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['SRC_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['SRC_PASSWORD'])).decode('utf-8', errors = 'ignore'))
            cmd += ' --connect "%s" %s --table %s --null-string \'\\\\N\' --null-non-string \'\\\\N\' --hive-import --hive-table temp.%s_import_temp --where "%s=%s" -m %s' % (src_db, columns, in_tab, proc['DST_TAB'], proc['COL_DIMENSION'], where_value, CONF.get('conf', 'sqoop_num'))
            cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if is_stop() is True:
                return None
            if None.get('conf', 'platform') == 'cdh':
                tmp_cmd = '%s -q "INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s PARTITION(%s=%s) SELECT %s FROM temp.%s_import_temp"' % (impala_bin, proc['DST_TAB'], proc['DST_USER'], proc['DST_TAB'], insert_column, proc['COL_DIMENSION'], where_value, opr_column, proc['DST_TAB'])
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            if CONF.get('conf', 'platform') == 'tdh':
                tmp_cmd = '%s -q "INSERT OVERWRITE TABLE %s.%s PARTITION(%s=%s) %s SELECT %s FROM temp.%s_import_temp"' % (impala_bin, proc['DST_USER'], proc['DST_TAB'], proc['COL_DIMENSION'], where_value, insert_column, opr_column, proc['DST_TAB'])
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            end_proc_logs(id, start, 3)
        elif proc['OPR_TYPE'] == 1:
            cmd = '%s import --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['SRC_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['SRC_PASSWORD'])).decode('utf-8', errors = 'ignore'))
            cmd += ' --connect "%s" %s --table %s --null-string \'\\\\N\' --null-non-string \'\\\\N\' --hive-import --hive-table temp.%s_import_temp -m %s' % (src_db, columns, in_tab, proc['DST_TAB'], CONF.get('conf', 'sqoop_num'))
            cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if is_stop() is True:
                return None
            if None.get('conf', 'platform') == 'cdh':
                tmp_cmd = '%s -q "INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s select %s from temp.%s_import_temp"' % (impala_bin, proc['DST_TAB'], proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'])
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                elif CONF.get('conf', 'platform') == 'tdh':
                    tmp_cmd = '%s -q "INSERT OVERWRITE %s.%s%s select %s from temp.%s_import_temp"' % (impala_bin, proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'])
                    LOGGER.info(tmp_cmd)
                    returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
                    else:
                        end_proc_logs(id, start, 3)
        if is_stop() is True:
            return None
        tmp_cmd = None % (hive_bin, proc['DST_TAB'])
        LOGGER.info(tmp_cmd)
        returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
        if returncode != 0:
            succ_flag = False
        elif OPR_TYPE == '2':
            dst_db = proc['DST_DB']
            dst_user = proc['DST_USER']
            dst_tab = proc['DST_TAB']
            out_tab = dst_tab
            opr_columns = '*'
            columns = ''
            if proc['DST_TYPE'] == 1:
                dst_db = dst_db.replace(proc['DST_DATABASE_NAME'], proc['DST_USER'])
            if proc['DST_TYPE'] == 4:
                dst_user = str(dst_user).upper()
                dst_tab = str(dst_tab).upper()
                out_tab = '%s.%s' % (dst_user, dst_tab)
            if proc['OPR_COLUMN'] is not None and proc['OPR_COLUMN'] != '':
                opr_columns = proc['OPR_COLUMN']
                if proc['SRC_TYPE'] == 4:
                    opr_columns = str(opr_columns).upper()
                if proc['OPR_TYPE'] == 3:
                    opr_columns = '%s,%s' % (opr_columns, proc['COL_DIMENSION'])
                columns = '--columns "%s"' % opr_columns
            if proc['OPR_TYPE'] == 2 or proc['OPR_TYPE'] == 3:
                tmp_cmd = '%s -e "drop table if exists temp.%s_export_temp purge;create table temp.%s_export_temp as select %s from %s.%s where %s=%s"' % (hive_bin, proc['SRC_TAB'], proc['SRC_TAB'], opr_columns, proc['SRC_USER'], proc['SRC_TAB'], proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if is_stop() is True:
                    return None
                tmp_cmd = None % (CONF.get('conf', 'run_bin'), dst_db, proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'), dst_user, dst_tab, proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if is_stop() is True:
                    return None
                None += '%s export --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'))
                cmd += ' --connect "%s" %s --table %s --hcatalog-database temp --hcatalog-table %s_export_temp -m %s' % (dst_db, columns, out_tab, proc['SRC_TAB'], CONF.get('conf', 'sqoop_num'))
                cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
                LOGGER.info(cmd)
                returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                elif proc['OPR_TYPE'] == 1:
                    tmp_cmd = '%s -e "drop table if exists temp.%s_export_temp purge;create table temp.%s_export_temp as select %s from %s.%s"' % (hive_bin, proc['SRC_TAB'], proc['SRC_TAB'], opr_columns, proc['SRC_USER'], proc['SRC_TAB'])
                    LOGGER.info(tmp_cmd)
                    returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
                    if is_stop() is True:
                        return None
                    tmp_cmd = None % (CONF.get('conf', 'run_bin'), dst_db, proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'), dst_user, dst_tab)
                    LOGGER.info(tmp_cmd)
                    returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
                    if is_stop() is True:
                        return None
                    cmd = None % (CONF.get('conf', 'run_bin'), proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'))
                    cmd += ' --connect "%s" %s --table %s --hcatalog-database temp --hcatalog-table %s_export_temp -m %s' % (dst_db, columns, out_tab, proc['SRC_TAB'], CONF.get('conf', 'sqoop_num'))
                    cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
                    LOGGER.info(cmd)
                    returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
            if is_stop() is True:
                return None
            tmp_cmd = None % (hive_bin, proc['SRC_TAB'])
            LOGGER.info(tmp_cmd)
            returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
    if succ_flag is True:
        succ()
    else:
        fail()


def read_config():
    global DB_CONF, CONF, LOG_FILE, LOGGER, MYSQL
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH)
    LOG_FILE = (sys.path[0] + '/' + CONF.get('conf', 'log_file')) % time.strftime('%Y%m%d', time.localtime())
    LOGGER = Logger(LOG_FILE).logger
    MYSQL = MysqlUtil(DB_CONF.get('db', 'host'), des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get('db', 'user'))), des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get('db', 'password'))), DB_CONF.get('db', 'database'), DB_CONF.get('db', 'port'))


def main():
    parallel_num = int(CONF.get('conf', 'parallel_num'))
    pool = multiprocessing.Pool(processes = parallel_num)
    
    try:
        procs = get_sqoop_proc(GROUP_ID)
        for proc in procs:
            pool.apply_async(execute_proc, args = (proc,))
    except Exception:
        None
        e = None
        None
        
        try:
            LOGGER.info('error: %s' % e)
            end_dataflow_logs(3)
        finally:
            e = None
            del e


    pool.close()
    pool.join()

if __name__ == '__main__':
    read_config()
    init_param()
    main()
__doc__ = '\n@name sqoop\xe6\x89\xa7\xe8\xa1\x8c\xe5\x99\xa8\n@author jiangbing\n@version 1.0.0\n@update_time 2018-06-25\n@comment 20180625 V1.0.0  jiangbing \xe6\x96\xb0\xe5\xbb\xba\n'
import sys
import getopt
import datetime
import time
import configparser
from utils.DBUtil import MysqlUtil
from utils.ProcUtil import ProcUtil
from utils.LogUtil import Logger
from pyDes import des, PAD_PKCS5
import base64
import multiprocessing
DB_CONF_PATH = sys.path[0] + '/conf/db.conf'
DB_CONF = None
CONF_PATH = sys.path[0] + '/conf/RunSqoop.conf'
CONF = None
MYSQL = None
LSH = None
VAR = { }
TYPE = None
OPR_TYPE = None
GROUP_ID = None
LOGGER = None
LOG_FILE = None
IMPALA_HOSTNODE = None
HIVE_HOSTNODE = None

def show_help():
    '''\xe6\x8c\x87\xe4\xbb\xa4\xe5\xb8\xae\xe5\x8a\xa9'''
    print('\n    -t                    \xe8\xb0\x83\xe5\xba\xa6\xe7\xb1\xbb\xe5\x9e\x8b\n    -v                    \xe4\xb8\xaa\xe6\x80\xa7\xe5\x8f\x82\xe6\x95\xb0\n    -l                    \xe6\xb5\x81\xe6\xb0\xb4\xe5\x8f\xb7\n    --opr_type            \xe6\x93\x8d\xe4\xbd\x9c\xe7\xb1\xbb\xe5\x9e\x8b\n    --group_id            Sqoop\xe5\x88\x86\xe7\xbb\x84ID\n    ')
    sys.exit()


def validate_input():
    '''\xe9\xaa\x8c\xe8\xaf\x81\xe5\x8f\x82\xe6\x95\xb0'''
    if TYPE is None:
        print('please input -t')
        LOGGER.info('please input -t')
        sys.exit(1)
    if LSH is None:
        print('please input -l')
        LOGGER.info('please input -l')
        sys.exit(1)
    if OPR_TYPE is None:
        print('please input --opr_type')
        LOGGER.info('please input --opr_type')
        sys.exit(1)
    if GROUP_ID is None:
        print('please input --group_id')
        LOGGER.info('please input --group_id')
        sys.exit(1)


def init_param():
    '''\xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe5\x8f\x82\xe6\x95\xb0'''
    global TYPE, LSH, OPR_TYPE, GROUP_ID, TYPE, LSH, OPR_TYPE, GROUP_ID
    
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'ht:v:l:', [
            'help',
            'type=',
            'var=',
            'lsh=',
            'opr_type=',
            'group_id='])
        if len(opts) == 0:
            show_help()
    except getopt.GetoptError:
        None
        None
        None
        show_help()
        sys.exit(1)

    for (name, value) in opts:
        if name in ('-h', '--help'):
            show_help()
        if name in ('-t', '--type'):
            TYPE = value
        if name in ('-v', '--var') and value != '':
            tmp = value.split('&')
            for item in tmp:
                t = item.split('=')
                if t[1] is not None and t[1] != '':
                    VAR[t[0]] = t[1]
        if name in ('-l', '--lsh'):
            LSH = value
        if name in ('--opr_type',):
            OPR_TYPE = value
        if name in ('--group_id',):
            GROUP_ID = value
    validate_input()


def get_sqoop_proc(id):
    '''\xe6\xa0\xb9\xe6\x8d\xaeid\xe8\x8e\xb7\xe5\x8f\x96\xe6\x95\xb0\xe6\x8d\xae\xe5\xa4\x84\xe7\x90\x86\xe7\xa8\x8b\xe5\xba\x8f'''
    sql = '\nSELECT a.SRC_USER,a.DST_USER,a.C_TABLE,a.P_KEY,a.TYPE_TAB,a.OPR_DIMENSION,a.COL_DIMENSION,a.OPR_COLUMN,a.OPR_TYPE,\nb.DB_LINK AS SRC_DB,b.DATABASE_NAME AS SRC_DATABASE_NAME,b.TYPE AS SRC_TYPE,b.USER_NAME AS SRC_USER_NAME,b.PASSWORD AS SRC_PASSWORD,\nIFNULL(c.TABLE_NAME,a.SRC_TAB) AS SRC_TAB,IFNULL(c.TABLE_NAME_CN,a.SRC_TAB) AS SRC_TAB_CN,\nd.DB_LINK AS DST_DB,d.DATABASE_NAME AS DST_DATABASE_NAME,d.TYPE AS DST_TYPE,d.USER_NAME AS DST_USER_NAME,d.PASSWORD AS DST_PASSWORD,\nIFNULL(e.TABLE_NAME,a.DST_TAB) AS DST_TAB,IFNULL(e.TABLE_NAME_CN,a.DST_TAB) AS DST_TAB_CN\nFROM t_job_sqoop_proc a\nLEFT JOIN t_srm_datasource b ON a.SRC_DB_ID=b.ID\nLEFT JOIN t_table c ON (a.SRC_TAB=c.TABLE_NAME AND a.SRC_USER=c.OWNER)\nLEFT JOIN t_srm_datasource d ON a.DST_DB_ID=d.ID\nLEFT JOIN t_table e ON (a.DST_TAB=e.TABLE_NAME AND a.DST_USER=e.OWNER)\nWHERE a.GROUP_ID=%s\n'
    if 'PROC' in VAR:
        sql += ' AND a.ID IN (%s)' % VAR['PROC']
    res = MYSQL.query(sql, (id,))
    return res


def get_impala_hostnode():
    '''\xe8\x8e\xb7\xe5\x8f\x96impala\xe4\xb8\xbb\xe6\x9c\xba'''
    sql = '\n        SELECT HOST_IP,USER_NAME,PASSWORD,DB_LINK FROM t_srm_datasource WHERE TYPE=5 LIMIT 1\n    '
    res = MYSQL.query(sql, ())
    LOGGER.info('get_hostnode: %s' % res)
    if len(res) > 0:
        return res[0]
    return None


def get_hive_hostnode():
    '''\xe8\x8e\xb7\xe5\x8f\x96impala\xe4\xb8\xbb\xe6\x9c\xba'''
    sql = '\n        SELECT HOST_IP,USER_NAME,PASSWORD,DB_LINK FROM t_srm_datasource WHERE TYPE=2 LIMIT 1\n    '
    res = MYSQL.query(sql, ())
    LOGGER.info('get_hostnode: %s' % res)
    if len(res) > 0:
        return res[0]
    return None


def start_proc_logs(PROC_NAME, PROC_DESC):
    '''\xe8\xae\xb0\xe5\xbd\x95\xe7\xa8\x8b\xe5\xba\x8f\xe6\x97\xa5\xe5\xbf\x97'''
    sql = '\n    insert into t_etl_proc_log(\n    `DETAIL_LOG_ID`,\n    `PROC_DESC`,\n    `PROC_NAME`,\n    `STAT_DATE`,\n    `START_TIME`,\n    `STATUS`,\n    `RUN_TYPE`\n    )values(%s,%s,%s,%s,%s,%s,%s)\n    '
    rq = 0
    if 'RQ' in VAR:
        rq = VAR['RQ']
    elif 'rq' in VAR:
        rq = VAR['rq']
    id = MYSQL.execute_sql(sql, (LSH, PROC_DESC, PROC_NAME, rq, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 1, TYPE))
    return id


def end_proc_logs(ID, start, STATUS):
    '''\xe4\xbf\xae\xe6\x94\xb9\xe7\xa8\x8b\xe5\xba\x8f\xe6\x97\xa5\xe5\xbf\x97'''
    COST_TIME = int(time.time()) - start
    sql = 'UPDATE t_etl_proc_log SET END_TIME=%s,COST_TIME=%s,STATUS=%s WHERE ID=%s'
    LOGGER.info(sql % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), COST_TIME, STATUS, ID))
    MYSQL.execute_sql(sql, (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), COST_TIME, STATUS, ID))
    if STATUS == 3 or STATUS == 4:
        end_dataflow_logs(STATUS)
    else:
        end_dataflow_logs()


def update_proc_logs(ID, PROCESS_ID):
    '''\xe4\xbf\xae\xe6\x94\xb9\xe7\xa8\x8b\xe5\xba\x8f\xe6\x97\xa5\xe5\xbf\x97'''
    sql = 'UPDATE t_etl_proc_log SET PROCESS_ID=%s WHERE ID=%s'
    MYSQL.execute_sql(sql, (PROCESS_ID, ID))


def is_stop():
    '''\xe6\x98\xaf\xe5\x90\xa6\xe7\xbb\x88\xe6\xad\xa2\xe6\x89\xa7\xe8\xa1\x8c\xe7\xa8\x8b\xe5\xba\x8f'''
    sql = 'SELECT ID FROM t_etl_proc_log WHERE DETAIL_LOG_ID=%s AND RUN_TYPE=%s AND STATUS=4'
    res = MYSQL.query(sql, (LSH, TYPE))
    return len(res) > 0


def end_dataflow_logs(STATUS = None):
    '''\xe4\xbf\xae\xe6\x94\xb9\xe8\x8a\x82\xe7\x82\xb9\xe6\x97\xa5\xe5\xbf\x97'''
    if STATUS is None:
        sql = 'UPDATE t_etl_dataflow_logs SET LOG_PATH=%s WHERE ID=%s'
        MYSQL.execute_sql(sql, (LOG_FILE, LSH))
    else:
        sql = 'UPDATE t_etl_dataflow_logs SET STATUS=%s, LOG_PATH=%s  WHERE ID=%s'
        MYSQL.execute_sql(sql, (STATUS, LOG_FILE, LSH))


def execute_proc(proc):
    '''sqoop\xe6\x89\xa7\xe8\xa1\x8c\xe5\x91\xbd\xe4\xbb\xa4'''
    global HIVE_HOSTNODE, IMPALA_HOSTNODE, HIVE_HOSTNODE, IMPALA_HOSTNODE
    if is_stop() is True:
        return None
    id = None(proc['SRC_TAB'], proc['SRC_TAB_CN'])
    start = int(time.time())
    succ_flag = True
    rq = 0
    if 'RQ' in VAR:
        rq = VAR['RQ']
    elif 'rq' in VAR:
        rq = VAR['rq']
    where_value = ''
    if proc['OPR_DIMENSION'] == 1:
        where_value = rq[0:4]
    elif proc['OPR_DIMENSION'] == 2:
        where_value = rq[0:6]
    elif proc['OPR_DIMENSION'] == 3:
        where_value = rq
    
    def add_log(pid, returncode, outs, errs):
        LOGGER.info('exec_cmd id:%s,  pid:%s, returncode:%s' % (id, pid, returncode))
        LOGGER.info('exec_cmd outs: %s' % outs)
        LOGGER.info('exec_cmd errs: %s' % errs)
        if returncode == -9:
            end_proc_logs(id, start, 4)

    
    def succ():
        end_proc_logs(id, start, 2)

    
    def fail():
        end_proc_logs(id, start, 3)

    
    def before(pid):
        update_proc_logs(id, pid)

    cmd = ''
    HIVE_HOSTNODE = get_hive_hostnode()
    hive_bin = CONF.get('conf', 'hive_bin')
    if hive_bin.count('%s') == 1:
        hive_bin = hive_bin % HIVE_HOSTNODE['DB_LINK']
    elif hive_bin.count('%s') == 3:
        hive_bin = hive_bin % (HIVE_HOSTNODE['DB_LINK'], HIVE_HOSTNODE['USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(HIVE_HOSTNODE['PASSWORD'])).decode('utf-8', errors = 'ignore'))
    impala_bin = hive_bin
    if CONF.get('conf', 'platform') == 'cdh':
        IMPALA_HOSTNODE = get_impala_hostnode()
        impala_bin = CONF.get('conf', 'impala_bin')
        if impala_bin.count('%s') == 1:
            impala_bin = impala_bin % IMPALA_HOSTNODE['HOST_IP']
        elif impala_bin.count('%s') == 2:
            impala_bin = impala_bin % (IMPALA_HOSTNODE['HOST_IP'], IMPALA_HOSTNODE['USER_NAME'])
        elif impala_bin.count('%s') == 3:
            impala_bin = impala_bin % (IMPALA_HOSTNODE['HOST_IP'], IMPALA_HOSTNODE['USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(IMPALA_HOSTNODE['PASSWORD'])).decode('utf-8', errors = 'ignore'))
    if OPR_TYPE == '1':
        src_db = proc['SRC_DB']
        src_user = proc['SRC_USER']
        src_tab = proc['SRC_TAB']
        in_tab = src_tab
        opr_column = '*'
        opr_columns = '*'
        columns = ''
        insert_column = ''
        if proc['SRC_TYPE'] == 1:
            src_db = src_db.replace(proc['SRC_DATABASE_NAME'], proc['SRC_USER'])
        if proc['SRC_TYPE'] == 4:
            src_user = str(src_user).upper()
            src_tab = str(src_tab).upper()
            in_tab = '%s.%s' % (src_user, src_tab)
        if proc['OPR_COLUMN'] is not None and proc['OPR_COLUMN'] != '':
            opr_column = proc['OPR_COLUMN']
            if proc['SRC_TYPE'] == 4:
                opr_column = str(opr_column).upper()
            columns = '--columns "%s"' % opr_column
            opr_columns = opr_column
            if proc['OPR_TYPE'] == 3:
                opr_columns = '%s,%s' % (opr_column, proc['COL_DIMENSION'])
                columns = '--columns "%s"' % opr_columns
        if opr_column != '*':
            insert_column = '(%s)' % opr_column
        tmp_cmd = '%s -e "drop table if exists temp.%s_import_temp purge;create table temp.%s_import_temp as select %s from %s.%s where 1=2;alter table temp.%s_import_temp set SERDEPROPERTIES(\'serialization.null.format\' = \'\\\\\\N\');"' % (hive_bin, proc['DST_TAB'], proc['DST_TAB'], opr_columns, proc['DST_USER'], proc['DST_TAB'], proc['DST_TAB'])
        LOGGER.info(tmp_cmd)
        returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
        if returncode != 0:
            succ_flag = False
        if is_stop() is True:
            return None
        if ((None, (None, (None, None))),)['OPR_TYPE'] == 2:
            cmd = '%s import --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['SRC_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['SRC_PASSWORD'])).decode('utf-8', errors = 'ignore'))
            cmd += ' --connect "%s" %s --table %s --null-string \'\\\\N\' --null-non-string \'\\\\N\' --hive-import --hive-table temp.%s_import_temp --where "%s=%s" -m %s' % (src_db, columns, in_tab, proc['DST_TAB'], proc['COL_DIMENSION'], where_value, CONF.get('conf', 'sqoop_num'))
            cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if is_stop() is True:
                return None
            if None.get('conf', 'platform') == 'cdh':
                tmp_cmd = '%s -q "INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s SELECT * FROM (SELECT %s FROM temp.%s_import_temp UNION ALL SELECT %s FROM %s.%s WHERE %s<>%s) T"' % (impala_bin, proc['DST_TAB'], proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'], opr_column, proc['DST_USER'], proc['DST_TAB'], proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            if CONF.get('conf', 'platform') == 'tdh':
                tmp_cmd = '%s -e "INSERT OVERWRITE TABLE %s.%s%s SELECT * FROM (SELECT %s FROM temp.%s_import_temp UNION ALL SELECT %s FROM %s.%s WHERE %s<>%s) T"' % (impala_bin, proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'], opr_column, proc['DST_USER'], proc['DST_TAB'], proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            end_proc_logs(id, start, 3)
        elif proc['OPR_TYPE'] == 3:
            cmd = '%s import --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['SRC_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['SRC_PASSWORD'])).decode('utf-8', errors = 'ignore'))
            cmd += ' --connect "%s" %s --table %s --null-string \'\\\\N\' --null-non-string \'\\\\N\' --hive-import --hive-table temp.%s_import_temp --where "%s=%s" -m %s' % (src_db, columns, in_tab, proc['DST_TAB'], proc['COL_DIMENSION'], where_value, CONF.get('conf', 'sqoop_num'))
            cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if is_stop() is True:
                return None
            if None.get('conf', 'platform') == 'cdh':
                tmp_cmd = '%s -q "INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s PARTITION(%s=%s) SELECT %s FROM temp.%s_import_temp"' % (impala_bin, proc['DST_TAB'], proc['DST_USER'], proc['DST_TAB'], insert_column, proc['COL_DIMENSION'], where_value, opr_column, proc['DST_TAB'])
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            if CONF.get('conf', 'platform') == 'tdh':
                tmp_cmd = '%s -q "INSERT OVERWRITE TABLE %s.%s PARTITION(%s=%s) %s SELECT %s FROM temp.%s_import_temp"' % (impala_bin, proc['DST_USER'], proc['DST_TAB'], proc['COL_DIMENSION'], where_value, insert_column, opr_column, proc['DST_TAB'])
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                
            end_proc_logs(id, start, 3)
        elif proc['OPR_TYPE'] == 1:
            cmd = '%s import --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['SRC_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['SRC_PASSWORD'])).decode('utf-8', errors = 'ignore'))
            cmd += ' --connect "%s" %s --table %s --null-string \'\\\\N\' --null-non-string \'\\\\N\' --hive-import --hive-table temp.%s_import_temp -m %s' % (src_db, columns, in_tab, proc['DST_TAB'], CONF.get('conf', 'sqoop_num'))
            cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
            LOGGER.info(cmd)
            returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
            if is_stop() is True:
                return None
            if None.get('conf', 'platform') == 'cdh':
                tmp_cmd = '%s -q "INVALIDATE METADATA temp.%s_import_temp;INSERT OVERWRITE %s.%s%s select %s from temp.%s_import_temp"' % (impala_bin, proc['DST_TAB'], proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'])
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                elif CONF.get('conf', 'platform') == 'tdh':
                    tmp_cmd = '%s -q "INSERT OVERWRITE %s.%s%s select %s from temp.%s_import_temp"' % (impala_bin, proc['DST_USER'], proc['DST_TAB'], insert_column, opr_column, proc['DST_TAB'])
                    LOGGER.info(tmp_cmd)
                    returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
                    else:
                        end_proc_logs(id, start, 3)
        if is_stop() is True:
            return None
        tmp_cmd = None % (hive_bin, proc['DST_TAB'])
        LOGGER.info(tmp_cmd)
        returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
        if returncode != 0:
            succ_flag = False
        elif OPR_TYPE == '2':
            dst_db = proc['DST_DB']
            dst_user = proc['DST_USER']
            dst_tab = proc['DST_TAB']
            out_tab = dst_tab
            opr_columns = '*'
            columns = ''
            if proc['DST_TYPE'] == 1:
                dst_db = dst_db.replace(proc['DST_DATABASE_NAME'], proc['DST_USER'])
            if proc['DST_TYPE'] == 4:
                dst_user = str(dst_user).upper()
                dst_tab = str(dst_tab).upper()
                out_tab = '%s.%s' % (dst_user, dst_tab)
            if proc['OPR_COLUMN'] is not None and proc['OPR_COLUMN'] != '':
                opr_columns = proc['OPR_COLUMN']
                if proc['SRC_TYPE'] == 4:
                    opr_columns = str(opr_columns).upper()
                if proc['OPR_TYPE'] == 3:
                    opr_columns = '%s,%s' % (opr_columns, proc['COL_DIMENSION'])
                columns = '--columns "%s"' % opr_columns
            if proc['OPR_TYPE'] == 2 or proc['OPR_TYPE'] == 3:
                tmp_cmd = '%s -e "drop table if exists temp.%s_export_temp purge;create table temp.%s_export_temp as select %s from %s.%s where %s=%s"' % (hive_bin, proc['SRC_TAB'], proc['SRC_TAB'], opr_columns, proc['SRC_USER'], proc['SRC_TAB'], proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if is_stop() is True:
                    return None
                tmp_cmd = None % (CONF.get('conf', 'run_bin'), dst_db, proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'), dst_user, dst_tab, proc['COL_DIMENSION'], where_value)
                LOGGER.info(tmp_cmd)
                returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                if is_stop() is True:
                    return None
                None += '%s export --username %s --password %s --verbose' % (CONF.get('conf', 'run_bin'), proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'))
                cmd += ' --connect "%s" %s --table %s --hcatalog-database temp --hcatalog-table %s_export_temp -m %s' % (dst_db, columns, out_tab, proc['SRC_TAB'], CONF.get('conf', 'sqoop_num'))
                cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
                LOGGER.info(cmd)
                returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
                if returncode != 0:
                    succ_flag = False
                elif proc['OPR_TYPE'] == 1:
                    tmp_cmd = '%s -e "drop table if exists temp.%s_export_temp purge;create table temp.%s_export_temp as select %s from %s.%s"' % (hive_bin, proc['SRC_TAB'], proc['SRC_TAB'], opr_columns, proc['SRC_USER'], proc['SRC_TAB'])
                    LOGGER.info(tmp_cmd)
                    returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
                    if is_stop() is True:
                        return None
                    tmp_cmd = None % (CONF.get('conf', 'run_bin'), dst_db, proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'), dst_user, dst_tab)
                    LOGGER.info(tmp_cmd)
                    returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
                    if is_stop() is True:
                        return None
                    cmd = None % (CONF.get('conf', 'run_bin'), proc['DST_USER_NAME'], des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(proc['DST_PASSWORD'])).decode('utf-8', errors = 'ignore'))
                    cmd += ' --connect "%s" %s --table %s --hcatalog-database temp --hcatalog-table %s_export_temp -m %s' % (dst_db, columns, out_tab, proc['SRC_TAB'], CONF.get('conf', 'sqoop_num'))
                    cmd += ' -- --default-character-set=utf-8 --delete-target-dir --direct'
                    LOGGER.info(cmd)
                    returncode = ProcUtil().single_pro(cmd, add_log, add_log, before)
                    if returncode != 0:
                        succ_flag = False
            if is_stop() is True:
                return None
            tmp_cmd = None % (hive_bin, proc['SRC_TAB'])
            LOGGER.info(tmp_cmd)
            returncode = ProcUtil().single_pro(tmp_cmd, add_log, add_log, before)
            if returncode != 0:
                succ_flag = False
    if succ_flag is True:
        succ()
    else:
        fail()


def read_config():
    global DB_CONF, CONF, LOG_FILE, LOGGER, MYSQL, DB_CONF, CONF, LOG_FILE, LOGGER, MYSQL
    DB_CONF = configparser.ConfigParser()
    DB_CONF.read(DB_CONF_PATH)
    CONF = configparser.ConfigParser()
    CONF.read(CONF_PATH)
    LOG_FILE = (sys.path[0] + '/' + CONF.get('conf', 'log_file')) % time.strftime('%Y%m%d', time.localtime())
    LOGGER = Logger(LOG_FILE).logger
    MYSQL = MysqlUtil(DB_CONF.get('db', 'host'), des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get('db', 'user'))), des(key = DB_CONF.get('conf', 'des_key'), padmode = PAD_PKCS5).decrypt(base64.b64decode(DB_CONF.get('db', 'password'))), DB_CONF.get('db', 'database'), DB_CONF.get('db', 'port'))


def main():
    parallel_num = int(CONF.get('conf', 'parallel_num'))
    pool = multiprocessing.Pool(processes = parallel_num)
    
    try:
        procs = get_sqoop_proc(GROUP_ID)
        for proc in procs:
            pool.apply_async(execute_proc, args = (proc,))
    except Exception:
        None
        e = None
        None
        
        try:
            LOGGER.info('error: %s' % e)
            end_dataflow_logs(3)
        finally:
            e = None
            del e


    pool.close()
    pool.join()

if __name__ == '__main__':
    read_config()
    init_param()
    main()