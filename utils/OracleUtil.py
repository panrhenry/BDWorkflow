# coding=utf-8
"""
oracleDB工具类
@author zyp
"""
import cx_Oracle


class OracleUtil:
    """oracle工具类"""

    def __init__(self, connon, is_dsn = False, user = None, password = None):
        # 读取配置文件
        # 连接数据库
        if is_dsn:
            #dsn方式连接
            self.conn = cx_Oracle.connect(user, password, dsn=connon)
        else:
            self.conn = cx_Oracle.connect(connon);
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def query(self, sql, param):
        """查询sql"""
        self.cursor.execute(sql, param)
        rows = self.cursor.fetchall()
        result_list = []
        if rows is None or len(rows) == 0:
            return result_list

        index_name_list = self.cursor.description
        idx_num = len(index_name_list)
        for res in rows:
            row = {}
            for i in range(idx_num):
                row[index_name_list[i][0]] = res[i]
            result_list.append(row)

        return result_list

    def execute_sql(self, sql, param):
        """执行sql"""
        self.cursor.execute(sql, param)
        self.conn.commit()
        # return self.cursor.lastrowid

    def db_query(self, host, user, password, database, port, sql, param):
        """连接并查询"""
        ss = user+"/"+password+"@"+host+":"+port+"/"+database
        conn = cx_Oracle.connect(ss)
        cursor = conn.cursor()
        cursor.execute(sql, param)
        rows = cursor.fetchall()
        result_list = []
        if rows is None or len(rows) == 0:
            return result_list

        index_name_list = cursor.description
        idx_num = len(index_name_list)
        for res in rows:
            row = {}
            for i in range(idx_num):
                row[index_name_list[i][0]] = res[i]
            result_list.append(row)

        cursor.close()
        conn.close()
        return result_list
