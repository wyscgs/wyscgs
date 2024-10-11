from get_token import *
import json
import requests
import dbconnection as dbcon
import write_log
import threading
import concurrent.futures
import datetime
import base64


# 将未上传的前100条数据状态置为2
def task_update2_mclz(key_name, exec_sql):
    write_log.write_log('update状态定时任务开始执行...')
    # 连接数据库
    cnx = dbcon.db_connect()
    if cnx is None:
        write_log.write_log(key_name + '连接数据库失败,定时任务退出')
        return False
    # 执行更新语句
    cursor = dbcon.exec_sql(cnx, exec_sql)
    if cursor is None:
        write_log.write_log(key_name + '执行SQL语句失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    cnx.commit()
    cursor.close()
    dbcon.db_disconnect(cnx)
    write_log.write_log('update状态定时任务完成')


def send_message(key_name, exec_sql, exec_sql1, type, url):
    write_log.write_log('%s 发送任务定时任务开始执行...' % key_name)
    # 创建一个字典来跟踪每个线程的执行次数
    thread_counts = {}
    dist_billnumber = select_distno(type)  # 获取暂估单编号

    token_data = get_token()  # 获取存储在Token.yaml文件中的token
    if not check_token_expiration(token_data):  # 检查token的有效性，如果为false 则重新获取token并存储在Token.yaml文件中
        savetoken()
        token_data = get_token()
        # 定义目标 URL
    token_str = token_data["token"]
    url = url + token_str
    # print(url)
    if type == 1 or type == 2:
        # 创建一个线程池，最大工作线程数可以根据需要进行调整
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # 准备一个列表来存储Future对象
            futures = []
            # 遍历 dist_billnumber 列表，并为每个元素生成一个索引和对应的值。start=1 参数表示索引从 1 开始。
            for i, bill_number_temp in enumerate(dist_billnumber, start=1):
                # 构建线程名称
                thread_name = "线程" + str(i % 10 + 1)
                # 提交任务到线程池，并传递订单号、线程名称和计数
                future = executor.submit(process_data, bill_number_temp, thread_name, i, url, exec_sql, exec_sql1, type)

                # 将Future对象添加到列表中
                futures.append(future)

                # 更新线程执行次数
                if thread_name in thread_counts:
                    thread_counts[thread_name] += 1
                else:
                    thread_counts[thread_name] = 1
        # 等待所有任务完成
        concurrent.futures.wait(futures)

        # 打印每个线程的执行次数
        for thread_name, count in thread_counts.items():
            write_log.write_log(f" {thread_name} 执行了 {count} 次任务")
    else:
        num = 1
        for billnumber in dist_billnumber:
            billnumber_detail = select_distno_detail(billnumber)
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                # 准备一个列表来存储Future对象
                futures = []
                # 遍历 dist_billnumber 列表，并为每个元素生成一个索引和对应的值。start=1 参数表示索引从 1 开始。
                for i, bill_number_temp in enumerate(billnumber_detail, start=1):
                    # 构建线程名称
                    thread_name = "线程" + str(i % 10 + 1)
                    # 提交任务到线程池，并传递订单号、线程名称和计数
                    future = executor.submit(process_data, bill_number_temp, thread_name, i, url, exec_sql, exec_sql1,
                                             type)

                    # 将Future对象添加到列表中
                    futures.append(future)

                    # 更新线程执行次数
                    if thread_name in thread_counts:
                        thread_counts[thread_name] += 1
                    else:
                        thread_counts[thread_name] = 1
                # 等待所有任务完成
            concurrent.futures.wait(futures)
            # 打印每个线程的执行次数
            for thread_name, count in thread_counts.items():
                write_log.write_log(f" {thread_name} 执行了 {count} 次任务")
            num = num + 1
    write_log.write_log('本次发送定时任务结束')


#  取130条状态为2的配送单
def select_distno(type_value):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('select_distno连接数据库失败')
        return False
    bill_number = []  # 初始化订单列表
    if type_value == 1:
        bill_type_list = "'dist','salereturn'"
    elif type_value == 2:
        bill_type_list = "'receipt','purchasereturn'"
    elif type_value == 3:
        bill_type_list = "'supplier_statement'"
    else:
        bill_type_list = "'breakfast_dinner_statement','fragmentary_statement'"
    # sql_str = """
    # SELECT DISTINCT bill_number
    # FROM (
    #     SELECT a.bill_number
    #     FROM wy_trans_to_kingdee a, e_dist b
    #     WHERE a.bill_number = b.bill_number
    #     AND a.bill_type IN ('dist')
    #     AND b.dist_date >= '2024-04-01'
    #     AND b.dist_date <= '2024-04-30'
    #     and a.trans_state='2'
    #     and b.warehouse_code  in  ('W0001','W0008','W0021','W0010')
    #     UNION ALL
    #     SELECT a.bill_number
    #     FROM wy_trans_to_kingdee a, e_sale_return b
    #     WHERE a.bill_number = b.bill_number
    #     AND a.bill_type IN ('salereturn')
    #     AND b.entry_date >= '2024-04-01'
    #     AND b.entry_date <= '2024-04-30'
    #     and a.trans_state='2'
    #     and b.warehouse_code  in  ('W0001','W0008','W0021','W0010')
    # ) a
    # """
    # sql_str = """
    # SELECT DISTINCT bill_number
    # FROM (
    #     SELECT bill_number
    #     FROM wy_trans_to_kingdee a
    #     WHERE bill_type IN ({})
    #     and trans_state='2'
    #     ORDER BY create_time
    # ) a
    # """
    #     sql_str = """
    # 	  SELECT DISTINCT bill_number
    #     FROM (
    #         SELECT a.bill_number
    #         FROM wy_trans_to_kingdee a, e_receipt b
    #         WHERE a.bill_number = b.bill_number
    #         AND a.bill_type IN ('receipt')
    #         AND b.receipt_time >= '2024-04-01'
    # --         AND b.receipt_time <= '2024-04-30'
    #         and a.trans_state='2'
    #         and b.warehouse_code  in  ('W0001','W0008','W0021','W0010')
    #         UNION ALL
    #         SELECT a.bill_number
    #         FROM wy_trans_to_kingdee a, e_purchase_return b
    #         WHERE a.bill_number = b.bill_number
    #         AND a.bill_type IN ('purchasereturn')
    #         AND b.occur_date >= '2024-04-01'
    # --         AND b.occur_date <= '2024-04-30'
    #         and a.trans_state='2'
    #         and b.warehouse_code  in  ('W0001','W0008','W0021','W0010')
    #     ) a
    #     """
    sql_str = """
    SELECT DISTINCT bill_number
    FROM (
        SELECT bill_number
        FROM wy_trans_to_kingdee a
        WHERE bill_type IN ({})
        and trans_state='2'
        ORDER BY create_time
    ) a
    """
    formatted_sql = sql_str.format(bill_type_list)
    # print(formatted_sql)
    try:
        cursor = cnx.cursor()
        cursor.execute(formatted_sql)  # 创建游标
        if cursor is None:
            write_log.write_log('select_distno执行语句失败')
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        results = cursor.fetchall()  # 数据库查询结果中获取所有剩余行
        for row in results:
            bill_number.append(row[0])  # 循环将结果加入bill_number列表中
        cursor.close()
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return bill_number
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        print(f"An error occurred: {str(e)}")


def select_distno_detail(billnumber):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('select_distno_detail连接数据库失败')
        return False
    bill_number = []  # 初始化订单列表
    sql_str = """
	SELECT  DISTINCT  b.bill_number
	from  e_account b,e_warehouse_org d
	where    b.warehouse_code=d.wrh_code 
	and  b.rec_bill_number=%s
    """
    try:
        cursor = cnx.cursor()
        cursor.execute(sql_str, (billnumber,))  # 创建游标
        if cursor is None:
            write_log.write_log('select_distno_detail执行语句失败')
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        results = cursor.fetchall()  # 数据库查询结果中获取所有剩余行
        for row in results:
            bill_number.append(row[0])  # 循环将结果加入bill_number列表中
        cursor.close()
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return bill_number
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        print(f"An error occurred: {str(e)}")


def process_data(bill_number, thread_name, i, url, exec_sql, exec_sql1, type):
    if type == 4:
        send_data = get_ticketing_data(bill_number, thread_name, i, exec_sql, exec_sql1)
        result = post_data_ticket(send_data, bill_number, i, thread_name, url)
    elif type == 3:
        send_data = get_additional_data(bill_number, thread_name, i, exec_sql, exec_sql1)
        result = post_data_ticket(send_data, bill_number, i, thread_name, url)
    else:
        send_data = get_additional_data(bill_number, thread_name, i, exec_sql, exec_sql1)
        result = post_data(send_data, bill_number, i, thread_name, url)
    return result


def get_additional_data(bill_number, thread_name, count, exec_sql, exec_sql1):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: 接数据库失败'.format(thread_name))
        return False
    write_log.write_log('{}: 第{}个订单拼接字符串开始！'.format(thread_name, count))
    try:
        send_data = {
            "data": {}
        }
        entry = []
        cursor = cnx.cursor()
        cursor.execute(exec_sql, (bill_number,))
        if cursor is None:
            write_log.write_log('{}: cursor执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        # 拼接json
        results = cursor.fetchall()  # 数据库查询结果中获取所有剩余行
        ls = [description[0] for description in cursor.description]  # 获取列名
        for row in results:
            for i in range(len(ls)):
                send_data["data"][ls[i]] = row[i]  # 循环拼接["data"]层级的订单信息
        cursor.close()  # 关闭游标
        # print(send_data)
        cursor1 = cnx.cursor()
        cursor1.execute(exec_sql1, (bill_number,))
        if cursor1 is None:
            write_log.write_log('{}: cursor1执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        # 拼接细项
        results1 = cursor1.fetchall()  # 数据库查询结果中获取所有剩余行
        ls1 = [description[0] for description in cursor1.description]  # 获取列名
        # 循环拼接["entry"]层级的订单信息
        for row in results1:
            if row[3] == 0:  # 当 row[4]   quantity等于 0 时，跳出当前循环并开始下面的新循环
                continue
            temp = {}
            for i in range(len(ls1)):
                temp[ls1[i]] = row[i]  # 循环拼接["data"]层级的订单信息
            entry.append(temp)
        cursor1.close()  # 关闭游标
        send_data["data"]["entry"] = entry
        write_log.write_log('{}: 第{}个订单拼接字符串结束'.format(thread_name, count))
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        # print(json.dumps(send_data, indent=2, ensure_ascii=False))
        return send_data
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        print(f"An error occurred: {str(e)}")


def get_ticketing_data(bill_number, thread_name, count, exec_sql, exec_sql1):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: 接数据库失败'.format(thread_name))
        return False
    write_log.write_log('{}: 第{}个订单拼接字符串开始！'.format(thread_name, count))
    try:
        send_data = {
            "requestId": int(datetime.datetime.utcnow().timestamp() * 1000),
            "businessSystemCode": "HAIDING_SYS",
            "interfaceCode": "BILL.PUSH",
            "data": []
        }
        entry = []
        datatemp = {}
        cursor = cnx.cursor()
        cursor.execute(exec_sql, (bill_number,))
        if cursor is None:
            write_log.write_log('{}: cursor执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        # 拼接json
        results = cursor.fetchall()  # 数据库查询结果中获取所有剩余行
        ls = [description[0] for description in cursor.description]  # 获取列名
        for row in results:
            for i in range(len(ls)):
                datatemp[ls[i]] = row[i]  # 循环拼接["data"]层级的订单信息
        cursor.close()  # 关闭游标

        cursor1 = cnx.cursor()
        cursor1.execute(exec_sql1, (bill_number,))
        if cursor1 is None:
            write_log.write_log('{}: cursor1执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        # 拼接细项
        results1 = cursor1.fetchall()  # 数据库查询结果中获取所有剩余行
        ls1 = [description[0] for description in cursor1.description]  # 获取列名
        # 循环拼接["entry"]层级的订单信息
        for row in results1:
            temp = {}
            for i in range(len(ls1)):
                temp[ls1[i]] = row[i]  # 循环拼接["data"]层级的订单信息
            entry.append(temp)
        cursor1.close()  # 关闭游标
        datatemp["billDetail"] = entry
        send_data["data"].append(datatemp)
        # print(json.dumps(send_data, indent=2, ensure_ascii=False))
        # 对data字段进行base64编码
        encoded_data = base64.b64encode(json.dumps(send_data["data"]).encode('utf-8')).decode('utf-8')

        # 更新send_data中的data字段为编码后的值
        send_data["data"] = encoded_data

        write_log.write_log('{}: 第{}个订单拼接字符串结束'.format(thread_name, count))
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        # print(json.dumps(send_data, indent=2, ensure_ascii=False))
        return send_data
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        print(f"An error occurred: {str(e)}")


def post_data(json_data, billnumber, count, thread_name, url):
    # 创建锁
    lock = threading.Lock()
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: update{}连接数据库失败'.format(thread_name, billnumber))
        return False
    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }  # 放入token，定义传输的数据类型为json
    # 发送 POST 请求
    write_log.write_log('{}: 第{}个订单发送开始'.format(thread_name, count))
    response = requests.post(url, json=json_data, headers=headers)  # 发送数据
    # print(response.json())
    if 'data' in response.json():
        # 请求成功
        sql_str1 = """
        UPDATE  wy_trans_to_kingdee
        set trans_state ='1',remark='更新成功',finish_time=now()
        where  bill_number='{}'
        """.format(billnumber)
        # 连接数据库
        cursor1 = dbcon.exec_sql(cnx, sql_str1)
        if cursor1 is None:
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        sql_str2 = """
        UPDATE  wy_trans_to_kingdee_log
        set trans_state ='1',remark='更新成功',finish_time=now()
        where  bill_number='{}'
        """.format(billnumber)
        # 连接数据库
        cursor2 = dbcon.exec_sql(cnx, sql_str2)
        if cursor2 is None:
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        cnx.commit()
        write_log.write_log('{}: 第{}个订单update{}执行语句成功'.format(thread_name, count, billnumber))
        cursor1.close()
        cursor2.close()
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        write_log.write_log("{}: 第{}个订单Request successful,Response:".format(thread_name, count))
        write_log.write_log(response.json())
        write_log.write_log('{}: 第{}个订单发送结束'.format(thread_name, count))
        return True
    else:
        write_log.write_log('{}: 第{}个订单请求失败，{}'.format(thread_name, count, response.json()))
        write_log.write_log(json.dumps(json_data, ensure_ascii=False, indent=2))
        sql_str1 = """
        UPDATE  wy_trans_to_kingdee
        set trans_state ='3',remark='{}',finish_time=now()
        where  bill_number='{}'
        """.format(response.json()['message'], billnumber)
        # 连接数据库
        cursor1 = dbcon.exec_sql(cnx, sql_str1)
        if cursor1 is None:
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        sql_str2 = """
        UPDATE  wy_trans_to_kingdee_log
        set trans_state ='3',remark='{}',finish_time=now()
        where  bill_number='{}'
        """.format(response.json()['message'], billnumber)
        # 连接数据库
        cursor2 = dbcon.exec_sql(cnx, sql_str2)
        if cursor2 is None:
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        cnx.commit()
        cursor1.close()
        cursor2.close()
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return False


def post_data_ticket(json_data, billnumber, count, thread_name, url):
    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }  # 放入token，定义传输的数据类型为json
    # 发送 POST 请求
    write_log.write_log('{}: 第{}个订单发送开始'.format(thread_name, count))
    response = requests.post(url, json=json_data, headers=headers)  # 发送数据
    if response.json()["message"] == "开票申请单推送成功":
        # 请求成功
        write_log.write_log("{}: 第{}个订单Request successful,Response:".format(thread_name, count))
        write_log.write_log(response.json())
        write_log.write_log('{}: 第{}个订单发送结束'.format(thread_name, count))
        return True
    else:
        decoded_json = decode_base64_json(json_data['data'])
        write_log.write_log('{}: 第{}个订单请求失败，{}'.format(thread_name, count, response.json()))
        write_log.write_log(json.dumps(decoded_json, ensure_ascii=False, indent=2))
        return False


def update_statement(billnumber, state, remark):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('update {}连接数据库失败'.format(billnumber))
        return False
    sql_str1 = """
   UPDATE  wy_trans_to_kingdee
   set trans_state ='{}',remark = '{}'
   where  bill_number='{}'
   """.format(state, remark, billnumber)
    # 连接数据库
    cursor1 = dbcon.exec_sql(cnx, sql_str1)
    if cursor1 is None:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return False
    sql_str2 = """
   UPDATE  wy_trans_to_kingdee_log
   set trans_state ='{}',remark = '{}'
   where  bill_number='{}'
   """.format(state, remark, billnumber)
    # 连接数据库
    cursor2 = dbcon.exec_sql(cnx, sql_str2)
    if cursor2 is None:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return False
    cnx.commit()
    write_log.write_log('{}: update {}对账单执行语句成功'.format(thread_name, billnumber))
    cursor1.close()
    cursor2.close()
    dbcon.db_disconnect(cnx)  # 关闭数据库连接


def decode_base64_json(encoded_data):
    # 解码 Base64 编码的数据
    decoded_data = base64.b64decode(encoded_data)
    # 解析 JSON 数据
    decoded_json_data = json.loads(decoded_data)
    return decoded_json_data


if __name__ == '__main__':
    write_log.write_log('开始')
    # get_additional_data('XS20240227002548', '线程1', '1')
    write_log.write_log('结束')
