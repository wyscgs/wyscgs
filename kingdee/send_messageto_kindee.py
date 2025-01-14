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
    dist_billnumber = select_distno(type)  # 获取订单编号

    token_data = get_token()  # 获取存储在Token.yaml文件中的token
    if not check_token_expiration(token_data):  # 检查token的有效性，如果为false 则重新获取token并存储在Token.yaml文件中
        savetoken()
        token_data = get_token()
        # 定义目标 URL
    token_str = token_data["token"]
    url = url + token_str
    # print(url)
    if type == 1 or type == 2 or type == 5:
        # 创建一个线程池，最大工作线程数可以根据需要进行调整
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            # 准备一个列表来存储Future对象
            futures = []
            # 遍历 dist_billnumber 列表，并为每个元素生成一个索引和对应的值。start=1 参数表示索引从 1 开始。
            for i, bill_number_temp in enumerate(dist_billnumber, start=1):
                # 构建线程名称
                thread_name = "线程" + str(i % 20 + 1)
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
            write_log.write_log('正在上传的对账单号为：{}'.format(billnumber))
            billnumber_detail = select_distno_detail(billnumber)
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                # 准备一个列表来存储Future对象
                futures = []
                # 用于标记是否需要中断当前账单的处理
                interrupt_flag = False
                # 遍历 dist_billnumber 列表，并为每个元素生成一个索引和对应的值。start=1 参数表示索引从 1 开始。
                for i, bill_number_temp in enumerate(billnumber_detail, start=1):
                    # 构建线程名称
                    thread_name = "线程" + str(i % 20 + 1)
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
                # 检查每个任务的结果
                for future in futures:
                    try:
                        result = future.result()
                        if not result:
                            interrupt_flag = True
                            break
                    except Exception as e:
                        # 处理异常，例如打印错误或记录日志
                        print(f"账单: {billnumber} 上传失败, error: {e}")
                # 等待所有任务完成
                # concurrent.futures.wait(futures)
                # 如果需要中断当前账单的处理，则设置账单状态并跳出循环
                if interrupt_flag:
                    update_statement(billnumber, 3, "上传失败")
                    continue
                else:
                    update_statement(billnumber, 1, "上传成功")
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
        formatted_sql = """
            SELECT DISTINCT bill_number
            FROM (
                SELECT a.bill_number
                FROM wy_trans_to_kingdee a
                JOIN e_dist b ON a.bill_number = b.bill_number
                WHERE a.bill_type = 'dist'
                and a.trans_state='2'
                and if(date_format(b.create_time,'%Y-%m-%d')<=date_format(date_format(b.dist_date,'%Y-%m-%d'),'%Y-%m-%d'),
                date_format(b.dist_date,'%Y-%m-%d')>='2024-04-01' and date_format(b.dist_date,'%Y-%m-%d')<='2024-04-30',
                date_format(b.create_time,'%Y-%m-%d')>='2024-04-01' and date_format(b.create_time,'%Y-%m-%d')<='2024-04-30')
                AND b.warehouse_code IN ('W0001', 'W0008', 'W0021', 'W0010', 'W0024')
                UNION ALL
                SELECT a.bill_number
                FROM wy_trans_to_kingdee a, e_sale_return b
                WHERE a.bill_number = b.bill_number
                AND a.bill_type IN ('salereturn')
                and a.trans_state='2'
                AND date_format(b.receive_time,'%Y-%m-%d')>='2024-04-01'
                AND date_format(b.receive_time,'%Y-%m-%d')<='2024-04-30'
                and b.warehouse_code  in  ('W0001','W0008','W0021','W0010','W0024')
            ) a		
        """
    elif type_value == 2:
        formatted_sql = """
            SELECT  DISTINCT   a.bill_number
            from   wy_trans_to_kingdee a,e_account b
            where a.bill_number=b.bill_number
            AND a.bill_type IN ('receipt','purchasereturn')
            and    date_format(b.occur_time,'%Y-%m-%d')>='2024-04-01'
            and    date_format(b.occur_time,'%Y-%m-%d')<='2024-04-30'
            and b.warehouse_code  in  ('W0001','W0008','W0021','W0010','W0024')
            """
    elif type_value == 3:
        formatted_sql = """

        """
    elif type_value == 5:
        formatted_sql = """
        SELECT  distinct bill_number 
        FROM wy_trans_to_kingdee 
        WHERE bill_type IN ('dist', 'salereturn')
        and statement_state='5'  
        """
    else:
        formatted_sql = """
        SELECT DISTINCT bill_number
        FROM (
            SELECT a.bill_number
            FROM wy_trans_to_kingdee a,e_breakfast_dinner_statement b ,e_school c
            where  a.bill_number = b.bill_number  and  b.school_code = c.code
            and a.bill_type = 'breakfast_dinner_statement'
            AND b.rec_date >= '2024-04-01'
            AND b.rec_date <= '2024-04-30'
            and a.trans_state='3'
            and c.wrh_code  in  ('W0001', 'W0008', 'W0021', 'W0010', 'W0024')
            union all
            SELECT a.bill_number
            FROM wy_trans_to_kingdee a,e_fragmentary_statement d ,e_school c
            WHERE  a.bill_number = d.bill_number and  d.school_code = c.code
            and a.bill_type = 'fragmentary_statement'
            AND d.rec_date >= '2024-04-01'
            AND d.rec_date <= '2024-04-30'
            and a.trans_state='3'
            and c.wrh_code  in  ('W0001', 'W0008', 'W0021', 'W0010', 'W0024')
        ) as m
        """
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
	from  e_account b
	where   b.rec_bill_number=%s
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
    try:
        if type == 4:
            send_data = get_ticketing_data(bill_number, thread_name, i, exec_sql, exec_sql1)
        elif type == 5:
            send_data = get_ticketing_data_return(bill_number, thread_name, i, exec_sql, exec_sql1)
        else:
            send_data = get_additional_data(bill_number, thread_name, i, exec_sql, exec_sql1)

        if type == 3 or type == 4:
            result = post_data_ticket(send_data, bill_number, i, thread_name, url)
        elif type == 5:
            result = post_data_ticket_return(send_data, bill_number, i, thread_name, url)
        else:
            result = post_data(send_data, bill_number, i, thread_name, url)
        return result
    except Exception as e:
        # 处理异常，例如记录日志或返回错误信息
        print(f"process_data处理 {bill_number} 时发生错误: {e}")
        return False


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
        # print(exec_sql)
        cursor = cnx.cursor()
        cursor.execute(exec_sql, (bill_number, bill_number))
        if cursor is None:
            write_log.write_log('{}: cursor执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        # 拼接json
        results = cursor.fetchall()  # 数据库查询结果中获取所有剩余行
        # 如果为空,直接返回错误
        if len(results) == 0:
            write_log.write_log('{}线程{}: cursor执行语句失败'.format(thread_name, bill_number))
            update_table("wy_trans_to_kingdee", "trans_state", "3", "remark", "金额为0，不需上传！", "finish_time",
                         bill_number)
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        ls = [description[0] for description in cursor.description]  # 获取列名
        for row in results:
            for i in range(len(ls)):
                send_data["data"][ls[i]] = row[i]  # 循环拼接["data"]层级的订单信息
        cursor.close()  # 关闭游标
        # print(send_data)
        cursor1 = cnx.cursor()
        # print(exec_sql1)
        cursor1.execute(exec_sql1, (bill_number, bill_number))
        if cursor1 is None:
            write_log.write_log('{}: cursor1执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        # 拼接细项
        results1 = cursor1.fetchall()  # 数据库查询结果中获取所有剩余行
        ls1 = [description[0] for description in cursor1.description]  # 获取列名
        # 循环拼接["entry"]层级的订单信息
        for row in results1:
            # if row[3] == 0:  # 当 row[4]   quantity等于 0 时，跳出当前循环并开始下面的新循环
            #     continue
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


def get_ticketing_data_return(bill_number, thread_name, count, exec_sql, exec_sql1):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: 接数据库失败'.format(thread_name))
        return False
    write_log.write_log('{}: 第{}个订单拼接字符串开始！'.format(thread_name, count))
    try:
        send_data = {
            "requestId": int(datetime.datetime.utcnow().timestamp() * 1000),
            "businessSystemCode": "HAIDING_SYS",
            "interfaceCode": "BILL.WITHDRAW",
            "data": {
                "orgCode": "010H0J",
                "serialNos": []
            }
        }
        serialNos = []
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
                serialNos.append(row[i])  # 循环拼接["data"]层级的订单信息
        cursor.close()  # 关闭游标
        send_data["data"]["serialNos"] = serialNos
        # print(json.dumps(send_data, indent=2, ensure_ascii=False))
        # 对data字段进行base64编码
        encoded_data = base64.b64encode(json.dumps(send_data["data"]).encode('utf-8')).decode('utf-8')

        # 更新send_data中的data字段为编码后的值
        send_data["data"] = encoded_data

        # print(json.dumps(send_data, indent=2, ensure_ascii=False))
        write_log.write_log('{}: 第{}个订单拼接字符串结束'.format(thread_name, count))
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return send_data
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        print(f"An error occurred: {str(e)}")


def post_data(json_data, billnumber, count, thread_name, url):
    # print(json_data)
    #  创建锁
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
    # print(response.status_code)
    # print(response.text)
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
        write_log.write_log("{}: 第{}个订单Request successful,Response:{}".format(thread_name, count, response.json()))
        write_log.write_log('{}: 第{}个订单发送结束'.format(thread_name, count))
        return True
    else:
        write_log.write_log('{}: 第{}个订单{}请求失败，{}'.format(thread_name, count, billnumber, response.json()))
        write_log.write_log('{}:{}'.format(billnumber, json.dumps(json_data, ensure_ascii=False, indent=2)))
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


def post_data_ticket(json_data, bill_number, count, thread_name, url):
    # print(json_data)
    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }  # 放入token，定义传输的数据类型为json
    try:
        # 发送 POST 请求
        write_log.write_log('{}: 第{}个订单{}发送开始'.format(thread_name, count, bill_number))
        response = requests.post(url, json=json_data, headers=headers)  # 发送数据
        if response.json()["message"] == "开票申请单推送成功" or 'data' in response.json():
            write_log.write_log("{}: 第{}个订单{}Request successful,Response:{}".format(thread_name, count, bill_number,
                                                                                        response.json()))
            update_table("wy_trans_to_kingdee", "statement_state", "1", "statement_remark", "开票申请单推送成功",
                         "statement_finsh_time", bill_number)
            write_log.write_log('{}: 第{}个订单{}发送结束'.format(thread_name, count, bill_number))
            return True
        else:
            # decoded_json = decode_base64_json(json_data['data'])
            write_log.write_log('{}: 第{}个订单{}请求失败，{}'.format(thread_name, count, bill_number, response.json()))
            update_table("wy_trans_to_kingdee", "statement_state", "3", "statement_remark", response.json()["message"],
                         "statement_finsh_time", bill_number)
            write_log.write_log(json.dumps(decode_data(json_data['data']), ensure_ascii=False, indent=4))
            return False
    except requests.exceptions.RequestException as e:
        write_log.write_log('{}: 第{}个订单{}请求异常，{}'.format(thread_name, count, bill_number, str(e)))
        return False


def post_data_ticket_return(json_data, bill_number, count, thread_name, url):
    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }  # 放入token，定义传输的数据类型为json
    try:
        # 发送 POST 请求
        write_log.write_log('{}: 第{}个订单{}撤回开始'.format(thread_name, count, bill_number))
        response = requests.post(url, json=json_data, headers=headers)  # 发送数据
        # 检查响应状态码是否表示成功
        if response.status_code == 200:
            return_data = response.json()
            # 'data'键是否存在于响应中,请求撤回成功
            if 'data' in return_data:
                return_data["data"] = decode_data(return_data["data"])
                write_log.write_log(return_data)
                update_table("wy_trans_to_kingdee", "statement_state", "7", "statement_remark", "开票申请单撤回成功",
                             "statement_finsh_time", bill_number)
                write_log.write_log('{}: 第{}个订单{}撤回结束'.format(thread_name, count, bill_number))
                return True
            else:
                write_log.write_log(
                    '{}: 第{}个订单{}撤回失败，{}'.format(thread_name, count, bill_number, response.json()))
                update_table("wy_trans_to_kingdee", "statement_state", "6", "statement_remark",
                             response.json()["message"],
                             "statement_finsh_time", bill_number)
                write_log.write_log(json.dumps(decode_data(json_data['data']), ensure_ascii=False, indent=4))
                return False
        else:
            # 如果响应状态码不是200，记录错误或进行其他处理
            print(f"Error: Received status code {response.status_code} from server.")
            # 您还可以选择打印出响应内容以便调试
            print(response.text)
            return False
    except requests.exceptions.RequestException as e:
        write_log.write_log('{}: 第{}个订单{}撤回异常，{}'.format(thread_name, count, bill_number, str(e)))
        return False


def update_statement(billnumber, state, remark):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('update {}连接数据库失败'.format(billnumber))
        return False
    sql_str1 = """
   UPDATE  wy_trans_to_kingdee
   set trans_state ='{}',remark = '{}',finish_time=now()
   where  bill_number='{}'
   """.format(state, remark, billnumber)
    # 连接数据库
    cursor1 = dbcon.exec_sql(cnx, sql_str1)
    if cursor1 is None:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return False
    sql_str2 = """
   UPDATE  wy_trans_to_kingdee_log
   set trans_state ='{}',remark = '{}',finish_time=now()
   where  bill_number='{}'
   """.format(state, remark, billnumber)
    # 连接数据库
    cursor2 = dbcon.exec_sql(cnx, sql_str2)
    if cursor2 is None:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        return False
    cnx.commit()
    write_log.write_log('update {}对账单执行语句成功，{}'.format(billnumber, remark))
    cursor1.close()
    cursor2.close()
    dbcon.db_disconnect(cnx)  # 关闭数据库连接


def update_table(table_name, table_state, state, table_remark, remark, table_time, billnumber):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: update{}连接数据库失败'.format(table_name, billnumber))
        return False
    sql_str = """
    UPDATE  {}
    set {} ='{}',{}='{}',{}=now()
    where  bill_number='{}'
    """.format(table_name, table_state, state, table_remark, remark, table_time, billnumber)
    try:
        cursor = dbcon.exec_sql(cnx, sql_str)
        if cursor is None:
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        cnx.commit()
        cursor.close()
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        write_log.write_log(f"An error occurred: {str(e)}")


# 解码
def decode_data(encoded_data):
    try:
        # 尝试解码Base64编码的数据
        decoded_bytes = base64.b64decode(encoded_data)
        # 将解码后的字节序列转换为字符串
        decoded_str = json.loads(decoded_bytes.decode('utf-8'))
        decoded_str_detail = decoded_str[0]
        return decoded_str_detail
    except Exception as e:
        print(f"解码失败: {e}")
        return None


if __name__ == '__main__':
    write_log.write_log('开始')
    # get_additional_data('XS20240227002548', '线程1', '1')
    write_log.write_log('结束')
