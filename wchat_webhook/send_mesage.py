'''
发送群消息函数
by lx 2023-11-5

需要建立表
create table wy_webhook_key(
key_name varchar(50) not null, /*任务名称，英文 全部小写*/
hook_key varchar(100) not null, /*企业微信机器人的 hookkey*/
hook_cname varchar(100) not null, /*任务名称，中文名，用于显示*/
business_name varchar(100) not null, /*企业微信名称*/
talk_name varchar(100)  null, /*群名称*/
remark varchar(200)  null, /*备注信息*/
primary key(key_name));

'''

import requests
import dbconnection as dbcon
from datetime import datetime, timedelta
import write_log


def send_message_to_wechat_robot(key_name, exec_sql):
    # 获取当前日期时间,界定每天凌晨2点以前为上一天时间
    current_datetime = datetime.now()
    # 减去2小时
    new_datetime = current_datetime - timedelta(hours=2)
    # 将日期时间转换成字符串
    new_datetime_str = new_datetime.strftime('%Y-%m-%d')

    write_log.write_log(key_name + '定时任务开始执行...')
    # 连接数据库
    cnx = dbcon.db_connect()
    if cnx is None:
        write_log.write_log(key_name + '连接数据库失败,定时任务退出')
        return False
    # 执行查询出来的语句
    cursor = dbcon.exec_sql(cnx, exec_sql)
    if cursor is None:
        write_log.write_log(key_name + '执行SQL语句失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    # 循环遍历printline的结果集，查询结果集必须是printline列名，将结果集存入变量result
    result = ""
    # 获取查询结果集的每一行
    rows = cursor.fetchall()
    # 遍历每一行，并取出print_line列的值并打印
    for row in rows:
        printline = row[0]
        # print(printline)
        result = result + printline + "\n" + "\n"
        # print("result=" + result)

    sql_key = "select hook_key,hook_cname from wy_webhook_key where key_name = '" + key_name + "';"
    get_key_cursor = dbcon.exec_sql(cnx, sql_key)
    if get_key_cursor is None:
        write_log.write_log(key_name + '获取任务webhook_key失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    # 获取查询结果集的第一行
    first_row = get_key_cursor.fetchone()
    # 通过列名获取hook_key值
    hook_key = first_row[0]
    hook_cname = first_row[1]
    # 断开数据库连接
    dbcon.db_disconnect(cnx)

    # 测试hook_key
    # hook_key = '126a6e26-8a4d-4a28-b207-502102e2c4e4'
    # hook_cname = '采购收货信息'

    # 组合字符串  例如：yyyy-mm-dd 基地收货数据：
    result = new_datetime_str + " " + hook_cname + "：\n" + result
    write_log.write_log(key_name + 'result：' + result)
    # 定义webhook的URL地址
    url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=' + hook_key
    data = {"msgtype": "text",
            "text": {
                "content": result
            }
            }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        write_log.write_log(key_name + '定时任务执行完成')
    else:
        write_log.write_log(key_name + '定时任务执行失败')


def send_message_to_wechat_robot_product_info(key_name, exec_sql):
    write_log.write_log(key_name + '定时任务开始执行...')
    # 连接数据库
    cnx = dbcon.db_connect()
    if cnx is None:
        write_log.write_log(key_name + '连接数据库失败,定时任务退出')
        return False
    # 执行查询出来的语句
    cursor = dbcon.exec_sql(cnx, exec_sql)
    if cursor is None:
        write_log.write_log(key_name + '执行SQL语句失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    # 获取查询结果集的每一行
    results = cursor.fetchall()
    total = results[0][1]  # 商品总数
    image = results[1][1]  # 缺图片商品总数
    shelf_life_value = results[2][1]  # 缺保质期总数
    sql_key = "select hook_key,hook_cname from wy_webhook_key where key_name = '" + key_name + "';"
    get_key_cursor = dbcon.exec_sql(cnx, sql_key)
    if get_key_cursor is None:
        write_log.write_log(key_name + '获取任务webhook_key失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    # 获取查询结果集的第一行
    first_row = get_key_cursor.fetchone()
    # 通过列名获取hook_key值
    hook_key = first_row[0]
    hook_cname = first_row[1]
    # 断开数据库连接
    dbcon.db_disconnect(cnx)

    # 测试hook_key
    # hook_key = '126a6e26-8a4d-4a28-b207-502102e2c4e4'
    # hook_cname = '采购收货信息'

    # 组合字符串  例如：yyyy-mm-dd 基地收货数据：
    result_info = """
    经系统查询，目前在库销售商品合计 {} 个，无图片商品为 {} 个,无保质期的商品为 {} 个，请采购部同事跟进处理。
    详细信息可以在报表-资料-商品基础信息报表 中查询
    """.format(total, image, shelf_life_value)
    write_log.write_log(key_name + 'result：' + result_info)
    # 定义webhook的URL地址
    url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=' + hook_key
    data = {"msgtype": "text",
            "text": {
                "content": result_info
            }
            }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        write_log.write_log(key_name + '定时任务执行完成')
    else:
        write_log.write_log(key_name + '定时任务执行失败')


def send_message_to_wechat_robot_trial_supplier_info(key_name, exec_sql):
    write_log.write_log(key_name + '定时任务开始执行...')
    # 连接数据库
    cnx = dbcon.db_connect()
    if cnx is None:
        write_log.write_log(key_name + '连接数据库失败,定时任务退出')
        return False
    # 执行查询出来的语句
    cursor = dbcon.exec_sql(cnx, exec_sql)
    if cursor is None:
        write_log.write_log(key_name + '执行SQL语句失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    result_info = ""
    # 获取查询结果集的每一行
    results = cursor.fetchall()
    # 遍历每一行，并取出print_line列的值并打印
    for row in results:
        printline = row[0]
        # print(printline)
        result_info = result_info + printline + "\n" + "\n"

    sql_key = "select hook_key,hook_cname from wy_webhook_key where key_name = '" + key_name + "';"
    get_key_cursor = dbcon.exec_sql(cnx, sql_key)
    if get_key_cursor is None:
        write_log.write_log(key_name + '获取任务webhook_key失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    # 获取查询结果集的第一行
    first_row = get_key_cursor.fetchone()
    # 通过列名获取hook_key值
    hook_key = first_row[0]
    hook_cname = first_row[1]
    # 断开数据库连接
    dbcon.db_disconnect(cnx)

    # 测试hook_key
    # hook_key = '126a6e26-8a4d-4a28-b207-502102e2c4e4'
    # hook_cname = '采购收货信息'

    # 组合字符串  例如：yyyy-mm-dd 基地收货数据：
    write_log.write_log(key_name + 'result：' + result_info)
    # 定义webhook的URL地址
    url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=' + hook_key
    data = {"msgtype": "text",
            "text": {
                "content": result_info
            }
            }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        write_log.write_log(key_name + '定时任务执行完成')
    else:
        write_log.write_log(key_name + '定时任务执行失败')


def send_message_to_wechat_robot_purchase_catalog(key_name, exec_sql):
    write_log.write_log(key_name + '定时任务开始执行...')
    # 连接数据库
    cnx = dbcon.db_connect()
    if cnx is None:
        write_log.write_log(key_name + '连接数据库失败,定时任务退出')
        return False
    # 执行查询出来的语句
    cursor = dbcon.exec_sql(cnx, exec_sql)
    if cursor is None:
        write_log.write_log(key_name + '执行SQL语句失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    result_info = ""
    # 获取查询结果集的每一行
    results = cursor.fetchall()
    line = len(results)
    if line == 0:
        write_log.write_log(key_name + '：不存在缺采购目录的商品')
        dbcon.db_disconnect(cnx)
        return False
    # 遍历每一行，并取出print_line列的值并打印
    for row in results:
        printline = row[0]
        # print(printline)
        result_info = result_info + printline + "\n" + "\n"

    sql_key = "select hook_key,hook_cname from wy_webhook_key where key_name = '" + key_name + "';"
    get_key_cursor = dbcon.exec_sql(cnx, sql_key)
    if get_key_cursor is None:
        write_log.write_log(key_name + '获取任务webhook_key失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    # 获取查询结果集的第一行
    first_row = get_key_cursor.fetchone()
    # 通过列名获取hook_key值
    hook_key = first_row[0]
    hook_cname = first_row[1]
    # 断开数据库连接
    dbcon.db_disconnect(cnx)

    # 测试hook_key
    # hook_key = '126a6e26-8a4d-4a28-b207-502102e2c4e4'
    # hook_cname = '采购收货信息'

    # 组合字符串  例如：yyyy-mm-dd 基地收货数据：
    write_log.write_log(key_name + 'result：' + result_info)
    # 定义webhook的URL地址
    url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=' + hook_key
    data = {"msgtype": "text",
            "text": {
                "content": result_info
            }
            }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        write_log.write_log(key_name + '定时任务执行完成')
    else:
        write_log.write_log(key_name + '定时任务执行失败')


def get_time_str():
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_datetime
