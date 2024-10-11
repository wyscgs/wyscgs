import mysql.connector
import write_log


def db_connect():
    try:
        # 连接数据库
        cnx = mysql.connector.connect(user='wymbsqus', password='wy123!#1#Q',
                                      host='10.169.142.201', database='erp',
                                      connect_timeout=10, port=33313, charset='utf8')
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
        return None


def exec_ddl_sql(cnx, sql):
    # 执行SQL语句
    cursor = cnx.cursor()
    cursor.execute(sql)
    try:
        # 提交更改
        cnx.commit()
    except mysql.connector.Error as err:
        # 执行SQL语句出错，写日志
        write_log.write_log(f"Failed to execute SQL: {err}")
        return None
    finally:
        return cursor
