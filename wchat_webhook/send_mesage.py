"""
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

"""

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
    weight = results[3][1]  # 缺重量
    introduction = results[4][1]  # 缺说明
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
    经系统查询，目前在库销售商品合计 {} 个，无图片商品为 {} 个，无保质期的商品为 {} 个，无重量的商品为 {} 个，无说明的商品为 {} 个，以上缺失信息将影响后期数据分析正确性及仓库商品有效期管理，请采购部同事跟进处理。
    详细信息可以在报表-资料-商品基础信息报表 中查询
    """.format(total, image, shelf_life_value, weight, introduction)
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
    # 获取当前日期加2天和加3天
    two_days_later = datetime.now().date() + timedelta(days=2)
    three_days_later = datetime.now().date() + timedelta(days=3)
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
    if line <= 0:
        # write_log.write_log(key_name + '：不存在缺采购目录的商品')
        # dbcon.db_disconnect(cnx)
        # return False
        result_info = '{}—{}不存在缺采购目录的商品'.format(two_days_later, three_days_later)
    elif line >= 20:
        line_count = 0  # 计数器用于记录行数
        result_info = '{}—{}共计{}个商品无对应仓库的采购目录，以下为前20个商品的详细信息：'.format(two_days_later,
                                                                                                 three_days_later,
                                                                                                 line) + "\n" + "\n"
        # 遍历每一行，并取出print_line列的值并打印
        for row in results:
            printline = "配送日期：" + row[0] + ",仓库编码：" + row[1] + ",仓库名称：" + row[2] + ",商品编码：" + row[
                3] + ",商品名称：" + row[4]
            # print(printline)
            result_info = result_info + printline + "\n" + "\n"
            line_count += 1  # 每遍历一行，计数器加1

            if line_count >= 20:
                break
    else:
        result_info = '{}—{}共计{}个商品无对应仓库的采购目录，以下为详细信息：'.format(two_days_later, three_days_later,
                                                                                     line) + "\n" + "\n"
        # 遍历每一行，并取出print_line列的值并打印
        for row in results:
            printline = "配送日期：" + row[0] + ",仓库编码：" + row[1] + ",仓库名称：" + row[2] + ",商品编码：" + row[
                3] + ",商品名称：" + row[4]
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


def send_message_to_wechat_robot_send_wave_state_info(key_name, exec_sql):
    # 获取当前日期时间,界定每天22点时之后为为后一天日期
    current_datetime = datetime.now()
    # 加上2小时
    new_datetime = current_datetime + timedelta(hours=2)
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
    results = cursor.fetchall()
    line = len(results)
    # 遍历每一行，并取出print_line列的值并打印
    for row in results:
        wrh_code = row[0]
        wrh_name = row[1]
        bill_number = row[2]
        state = row[3]
        pickup_count = row[4]
        unlocks = row[5]
        unpicked = row[6]
        picked = row[7]
        checked = row[8]
        suspend = row[9]
        canceled = row[10]
        # print(printline)
        result = (
                result + '仓库编码:' + wrh_code + ',仓库名称:' + wrh_name + ',波次号：' + bill_number +
                ',状态：' + state + '。' + "\n" +
                '拣货指令数：' + str(pickup_count) + ',未占货:' + str(unlocks) + ',未拣货:' + str(unpicked) +
                ',拣货完成：' + str(picked) + ',已复核：' + str(checked) + ',已挂起：' + str(suspend) + ',已取消：' + str(
            canceled) +
                "\n" + "\n")
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
    result = '配送日期：' + new_datetime_str + '，超过10个订单的波次中，还有' + str(
        line) + '个波次未完成，具体如下 ' + "：\n" + result
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


def send_message_to_wechat_robot_send_dist_weight(key_name, exec_sql):
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
    expiration_date = results[0][0]  # 当天的日期
    day_dist_amount = results[0][1]  # 当天的金额
    day_dist_weight = results[0][2]  # 当天的重量
    week_date = results[0][3]  # 本周的开始日期和结束日期
    week_dist_amount = results[0][4]  # 本周的金额
    week_dist_weight = results[0][5]  # 本周的重量
    month_date = results[0][6]  # 本月的开始日期和结束日期
    month_dist_amount = results[0][7]  # 本月的金额
    month_dist_weight = results[0][8]  # 本月的重量
    semester_date = results[0][9]  # 本学期的开始日期和结束日期
    semester_dist_amount = results[0][10]  # 按学期的金额
    semester_dist_weight = results[0][11]  # 按学期的重量
    year_date = results[0][12]  # 按2024年累计的开始日期和结束日期
    year_dist_amount = results[0][13]  # 按2024年累计的金额
    year_dist_weight = results[0][14]  # 按2024年累计的重量
    all_dist_weight = results[0][15]  # 累计全部重量

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
    result_info = """截至学校配送日期 {} 16：00 ，当天配送金额为 {} 万元，重量为 {} 吨；
本周累计({})配送金额为 {} 万元，重量为 {} 吨；
本月累计({})配送金额为 {} 万元，重量为 {} 吨；
本学期累计({})配送金额为 {} 万元，重量为 {} 吨；
本年累计({})配送金额为 {} 万元，重量为 {} 吨；
自项目启动至 {} 累计配送重量为 {} 吨；
注意:数据保留小数点后两位四舍五入，不包含销售退货、紧急采购和供应商直送。""".format(expiration_date, day_dist_amount, day_dist_weight,
                                                                 week_date, week_dist_amount, week_dist_weight,
                                                                 month_date, month_dist_amount, month_dist_weight,
                                                                 semester_date, semester_dist_amount, semester_dist_weight,
                                                                 year_date, year_dist_amount, year_dist_weight,
                                                                 expiration_date, all_dist_weight)
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
