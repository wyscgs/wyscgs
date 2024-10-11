import pymysql

# 读写参数
mysql_params = {
    'host': '10.169.142.201',
    'user': 'wymbsqus',  # 数据库用户名
    'passwd': 'wy123!#1#Q',  # 用户密码
    'db': 'erp',  # 数据库的名字
    'port': 33313,  # 端口号
    'charset': 'utf8'  # 编码方式
}


def create_mysql_conn():
    # pymysql连接rebecca数据库
    conn = pymysql.connect(host=mysql_params['host'],
                           port=mysql_params['port'],
                           user=mysql_params['user'],
                           passwd=mysql_params['passwd'],
                           db=mysql_params['db'],
                           charset=mysql_params['charset'])

    return conn



