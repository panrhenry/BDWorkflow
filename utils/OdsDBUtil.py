# coding=utf-8
"""
DB工具类
@author jiangbing
"""
import pymysql
import pymssql
import time

class MysqlUtil:
    """mysql工具类"""
    def __init__(self, host, user, password, database, port):
        # 读取配置文件
        # 连接数据库
        self.conn = pymysql.connect(host, user, password, database, int(port), charset='utf8')
        self.cursor = self.conn.cursor()

    def _reConn(self, num=60, stime=3):  # 重试连接总次数为1天,这里根据实际情况自己设置
        _number = 0
        _status = True
        while _status and _number <= num:
            try:
                self.conn.ping()  # cping 校验连接是否异常
                _status = False
            except:
                if self._conn() == True:  # 重新连接,成功退出
                    _status = False
                    break
                _number += 1
                time.sleep(stime)

    def close(self):
        self.cursor.close()
        self.conn.close()

    def query(self, sql, param):
        """查询sql"""
        self._reConn()
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
        self._reConn()
        self.cursor.execute(sql, param)
        self.conn.commit()
        return self.cursor.lastrowid

    def execute_sql_sleep(self, sql, param):
        """执行sql"""
        self._reConn()
        self.cursor.execute(sql, param)
        id=int(self.conn.insert_id())
        self.conn.commit()
        return id

    def executemany(self, sql, param):
        """执行sql"""
        self._reConn()
        self.cursor.executemany(sql, param)
        self.conn.commit()

    def db_query(self, host, user, password, database, port, sql, param):
        """连接并查询"""
        self._reConn()
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


class SqlserverUtil:
    def __init__(self, host=None, user=None, password=None, database=None, port=None):
        try:
            print(host + ":" + str(port))
            print(user)
            print(password)
            print(database)
            self.conn = pymssql.connect(host=host + ":" + str(port), user=user, password=password, database=database ,login_timeout=5)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(str(e))

    def __del__(self):
        pass
        # if self.cursor != None:
        #     self.cursor.close()
        # if self.conn != None:
        #     self.conn.close()

    def query(self, sql, param):
        """查询sql"""
        try:
            # 在这里加上as_dict=True参数，是的返回值是字典类型，默认是列表，也可以在connect的时候加
            cursor = self.conn.cursor(as_dict=True)
            if param == None:
                cursor.execute(sql)
                rs = cursor.fetchall()
                cursor.close()
            else:
                cursor.execute(sql, param)
                rs = cursor.fetchall()
                cursor.close()
        except Exception as e:
            print(str(e))
            rs = ()
        return rs

    def execute_sql(self, sql, param):
        try:
            cursor = self.conn.cursor()
            n = cursor.execute(sql, param)
            self.conne.commit()
            cursor.close()
            return n
        except Exception as e:
            print(str(e))
            self.conn.rollback()
            cursor.close()
            return -1

