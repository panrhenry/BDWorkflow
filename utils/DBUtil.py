# coding=utf-8
"""
DB工具类
@author jiangbing
"""
import pymysql
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class MysqlUtil:
    """mysql工具类"""

    def __init__(self, host, user, password, database, port):
        # 初始化配置
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        # self.conn = pymysql.connect(host, user, password, database, int(port), charset='utf8')
        # self.cursor = self.conn.cursor()

    # def close(self):
    #     self.cursor.close()
    #     self.conn.close()

    def query(self, sql, param):
        """查询sql"""
        conn = pymysql.connect(self.host, self.user, self.password, self.database, int(self.port), charset='utf8')
        cursor = conn.cursor()
        cursor.execute(sql, param)
        rows = cursor.fetchall()
        # self.cursor.execute(sql, param)
        # rows = self.cursor.fetchall()
        result_list = []
        if rows is None or len(rows) == 0:
            return result_list

        # index_name_list = self.cursor.description
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

    def execute_sql(self, sql, param):
        """执行sql"""
        conn = pymysql.connect(self.host, self.user, self.password, self.database, int(self.port), charset='utf8')
        cursor = conn.cursor()
        cursor.execute(sql, param)
        conn.commit()
        lastrowid = cursor.lastrowid
        cursor.close()
        conn.close()
        return lastrowid

    def db_query(self, host, user, password, database, port, sql, param):
        """连接并查询"""
        conn = pymysql.connect(host, user, password, database, int(port), charset='utf8')
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


class OracleUtil:
    """oracle工具类"""

    def __init__(self, host, user, password, database, port):
        # 初始化配置
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.url = "%s/%s@%s:%s/%s" % (self.user, self.password, self.host, self.port, self.database)

    def query(self, sql, param):
        """执行查询"""
        import cx_Oracle
        conn = cx_Oracle.connect(self.url)
        cursor = conn.cursor()
        cursor.execute(sql, param)
        rows = cursor.fetchall()
        result_list = []
        if rows is None or len(rows) == 0:
            return result_list

        # index_name_list = self.cursor.description
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

    def execute_sql(self, sql, param):
        """执行sql"""
        import cx_Oracle
        conn = cx_Oracle.connect(self.url)
        cursor = conn.cursor()
        cursor.execute(sql, param)
        conn.commit()
        cursor.close()
        conn.close()

    def db_query(self, host, user, password, database, port, sql, param):
        """连接并查询"""
        import cx_Oracle
        url = "%s/%s@%s:%s/%s" % (user, password, host, port, database)
        conn = cx_Oracle.connect(url)
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
