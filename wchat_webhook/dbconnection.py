import mysql.connector
import write_log


def db_connect():
    try:
        # 连接数据库
        cnx = mysql.connector.connect(user='wymbsqus', password='wy123!#1#Q',
                                      host='10.169.142.201', database='erp', port=33313, connect_timeout=300)
        write_log.write_log(f"connect to MySQL:Success")
        return cnx
    except mysql.connector.Error as err:
        # 连接不成功，写日志并返回失败
        write_log.write_log(f"Failed to connect to MySQL: {err}")
        return None


def db_disconnect(cnx):
    # 断开数据库连接
    cnx.close()


def exec_sql(cnx, sql):
    try:
        # 执行SQL语句
        cursor = cnx.cursor()
        cursor.execute(sql)
        return cursor
    except mysql.connector.Error as err:
        # 执行SQL语句出错，写日志
        write_log.write_log(f"Failed to execute SQL: {err}")
        write_log.write_log(sql)
        return None
