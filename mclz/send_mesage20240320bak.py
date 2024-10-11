from get_token import *
import json
import requests
import dbconnection as dbcon
import write_log
import threading
import concurrent.futures


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


def task_datele_mclz(key_name, exec_sql):
    write_log.write_log('delete定时删除上传成功任务开始执行...')
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
    write_log.write_log('delete定时删除上传成功任务完成')


#  发送主程序
def send_message_to_mclz():
    write_log.write_log('发送定时任务开始执行...')
    # 创建一个字典来跟踪每个线程的执行次数
    thread_counts = {}
    # 获取订单号列表
    dist_billnumber = select_distno()

    # 获取token
    token_data = get_token()
    if not check_token_expiration(token_data):
        login_auth()
        token_data = get_token()
    token = token_data["token"]

    # 创建一个线程池，最大工作线程数可以根据需要进行调整
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # 准备一个列表来存储Future对象
        futures = []
        # 遍历 dist_billnumber 列表，并为每个元素生成一个索引和对应的值。start=1 参数表示索引从 1 开始。
        for i, bill_number in enumerate(dist_billnumber, start=1):
            # 构建线程名称
            thread_name = "线程" + str(i % 5 + 1)

            # 提交任务到线程池，并传递订单号、线程名称和计数
            future = executor.submit(process_data, bill_number, thread_name, i)

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

    write_log.write_log('本次发送定时任务结束')


#  取220条状态为2的配送单
def select_distno():
    bill_number = []  # 初始化订单列表
    sql_str = """
        SELECT  DISTINCT bill_number 
        from(
        select   bill_number
        from  wy_dist_to_mclz a
        where  state='2'
        ORDER BY create_time 
        limit 225
        ) a
        """
    try:
        cnx = dbcon.db_connect()  # 连接数据库
        if cnx is None:
            write_log.write_log('select_distno连接数据库失败')
            return False
        cursor = dbcon.exec_sql(cnx, sql_str)  # 创建游标
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


#  真正的执行函数
def process_data(bill_number, thread_name, count):
    # 获取拼接json
    send_data = get_additional_data(bill_number, thread_name, count)
    token_data = get_token()  # 获取存储在Token.yaml文件中的token
    # if not check_token_expiration(token_data):  # 检查token的有效性，如果为false 则重新获取token并存储在Token.yaml文件中
    #     login_auth()
    #     token_data = get_token()
    # 发送数据
    post_data(send_data, bill_number, token_data["token"], count, thread_name)


# 根据bill_number进行数据库查询，拼接json
def get_additional_data(bill_number, thread_name, count):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: 接数据库失败'.format(thread_name))
        return False
    write_log.write_log('{}: 第{}个订单拼接字符串开始！'.format(thread_name, count))
    try:
        send_data = {
            "data": {
                "url": "AddFoodfirmfillin",
                "paramdata": {
                    "FD_FoodfirmfillinfileInfos": [{
                        "FileTypeId": 2,
                        "FileName": "",
                        "FileUrl": ""
                    }],
                    "FD_FoodfirmfillindetailInfos": []
                }
            }
        }  # 初始化post的数据
        # 获取订单信息
        exec_sql = """
        select  a.bill_number DistNo, '南宁威耀集采配供链管理有限公司'  DistSupplyName,DATE_FORMAT(b.dist_Date, '%Y-%m-%d')  DistDate,
        food_safe_account  FirmID,d.contact_name  PurchaseMan,d.contact_name  CheckMan ,
        DATE_FORMAT(b.dist_Date, '%Y-%m-%d %H:%i:%S')  FillInDatetime
        from  wy_dist_to_mclz a ,e_dist b ,e_school d
        WHERE  a.bill_number=b.bill_number
        and   b.school_code=d.code 
        and   a.state='2'
        and   a.bill_number='{}'
        """.format(bill_number)
        cursor = dbcon.exec_sql(cnx, exec_sql)  # 创建游标
        if cursor is None:
            write_log.write_log('{}: cursor执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        results = cursor.fetchall()  # 数据库查询结果中获取所有剩余行
        row_count = len(results)  # 关闭数据库连接
        if row_count < 1:
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            write_log.write_log('{}订单已存在,不上传'.format(bill_number))
            return False
        # 拼接配送订单信息
        ls = [description[0] for description in cursor.description]  # 获取列名
        for row in results:
            for i in range(len(ls)):
                send_data["data"]["paramdata"][ls[i]] = row[i]  # 循环拼接["data"]["paramdata"]层级的配送订单信息
        cursor.close()  # 关闭游标
        # 获取订单的商品详情信息
        exec_sql2 = """
        		select  j.OrderQuantity,j.DeliveryQuantity,j.CheckQuantity,j.Price,j.ProductDate,j.ProductStandardCode,
        		j.GenericCode,j.GenericName,j.FoodTypeCode	,j.FoodTypeName	,j.Specification,j.Unit	,j.ShelfLife,
        		j.ProductFirmName,MAX(CONCAT('https://erp.nnwysc.com/eos-download/accessory',g.file_path)) ProductImageUrl
        		from  (
        		select  CONVERT(c.plan_quantity, FLOAT) OrderQuantity, CONVERT(c.quantity, FLOAT)  DeliveryQuantity,
                CONVERT(c.receive_quantity, FLOAT)   CheckQuantity,CONVERT(c.price, FLOAT) Price,
                MAX(CASE WHEN f.production_date IS NOT NULL THEN DATE_FORMAT(f.production_date, '%Y-%m-%d %H:%i:%S') ELSE DATE_FORMAT(DATE(b.dist_date - INTERVAL 1 DAY) ,'%Y-%m-%d %H:%i:%S')END) AS ProductDate,
                MAX(f.batch)  ProductStandardCode,h.code  GenericCode,REPLACE(h.name, '#', '')  GenericName,h.category_code  FoodTypeCode,
                h.category_name FoodTypeName, REPLACE(h.specification, '#', '') Specification,h.spec_unit Unit,
                CASE WHEN h.shelf_life_unit='month' THEN  h.shelf_life_value*30 
        		WHEN h.shelf_life_unit='year' THEN  h.shelf_life_value*365 
        		WHEN h.shelf_life_value is NULL THEN  3 
        		ELSE h.shelf_life_value END as  ShelfLife,
                MAX(f.supplier_name) ProductFirmName,h.uuid
        	    from   e_dist b,e_dist_line c ,e_dist_line_detail e  ,e_batch  f,e_product h
                WHERE  b.bill_number=c.dist_bill_number
                and   c.uuid=e.dist_line_uuid
                and   e.batch=f.batch
                and   b.warehouse_code=f.wrh_code
                and   c.product_code=h.code
                and   b.bill_number='{}'
        		group by  c.plan_quantity,c.quantity,c.receive_quantity ,c.price ,h.code  ,h.name  ,
                h.category_code  ,h.category_name , h.specification ,h.spec_unit ,h.shelf_life_value  
        		)  j  LEFT JOIN  e_accessory g
        		ON  j.uuid=g.owner_uuid
                group by  j.OrderQuantity,j.DeliveryQuantity,j.CheckQuantity,j.Price,j.ProductDate,j.ProductStandardCode,
                j.GenericCode,j.GenericName,j.FoodTypeCode,j.FoodTypeName	,j.Specification,j.Unit	,j.ShelfLife,j.ProductFirmName 
                """.format(bill_number)
        cursor2 = dbcon.exec_sql(cnx, exec_sql2)  # 创建游标
        if cursor2 is None:
            write_log.write_log('{}: cursor2执行语句失败'.format(thread_name))
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            return False
        # 拼接配送订单的商品信息
        results2 = cursor2.fetchall()  # 数据库查询结果中获取所有剩余行
        ls2 = [description[0] for description in cursor2.description]  # 获取列名
        for row2 in results2:
            product_info = {
                "FD_FoodInfo": {

                }
            }  # 初始化单个商品信息
            for i in range(len(ls2)):
                if (ls2[i] == "OrderQuantity" or ls2[i] == "DeliveryQuantity" or ls2[i] == "CheckQuantity"
                        or ls2[i] == "Price" or ls2[i] == "ProductDate" or ls2[i] == "ProductStandardCode"):
                    product_info[ls2[i]] = row2[i]
                    # 根据商品的批次号获取检验报告放放入商品信息product_info中
                    if ls2[i] == "ProductStandardCode":
                        # exec_sql3 = """
                        #         select  REPLACE(REPLACE(j.name, "'", ''),'#','') FileName,CONCAT('https://erp.nnwysc.com/eos-download/accessory',j.file_path) FileUrl
                        #          from   e_batch h,e_accessory j
                        #          WHERE h.batch=j.owner_uuid
                        #          and   h.batch='{}'
                        #          """.format(row2[i])
                        exec_sql3 = """
                        select  REPLACE(REPLACE(name, "'", ''),'#','') FileName,CONCAT('https://erp.nnwysc.com/eos-download/accessory',file_path) FileUrl
                        from  e_accessory   
                        where   owner_uuid='{}'			
                        """.format(row2[i])
                        cursor3 = dbcon.exec_sql(cnx, exec_sql3)  # 创建游标
                        if cursor3 is None:
                            write_log.write_log('{}: cursor3执行语句失败'.format(thread_name))
                            cursor2.close()
                            dbcon.db_disconnect(cnx)  # 关闭数据库连接
                            return False
                        results3 = cursor3.fetchall()  # 数据库查询结果中获取所有剩余行
                        file_temp = []  # 临时列表，将多个检验报告的字典放入临时列表
                        ls3 = [description[0] for description in cursor3.description]  # 获取列名
                        for row3 in results3:
                            temp = {}
                            for k in range(len(ls3)):
                                temp[ls3[k]] = row3[k]  # 循环将将检验报告名称和链接放入临时字典temp，方便放入file_temp列表
                            file_temp.append(temp)
                            # 拼装到FD_FoodfirmfillindtlfileInfos列表
                        product_info["FD_FoodfirmfillindtlfileInfos"] = file_temp  # 临时列表放入product_info中
                        cursor3.close()
                else:
                    product_info["FD_FoodInfo"][ls2[i]] = row2[i]
            # 拼装到FD_FoodfirmfillindetailInfos列表
            send_data["data"]["paramdata"]["FD_FoodfirmfillindetailInfos"].append(product_info)
        cursor2.close()  # 关闭游标
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        write_log.write_log('{}: 第{}个订单拼接字符串结束'.format(thread_name, count))
        return send_data
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        print(f"An error occurred: {str(e)}")


#  发送函数
def post_data(json_data, billnumber, token_str, count, thread_name):
    # 创建锁
    # lock = threading.Lock()
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: update{}连接数据库失败'.format(thread_name, billnumber))
        return False
    headers = {
        "token": token_str,
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }  # 放入token，定义传输的数据类型为json
    # 定义目标 URL
    url = "http://36.137.36.34:9015/v1/Third/ThirdRequest"
    # 发送 POST 请求
    try:
        write_log.write_log('{}: 第{}个订单发送开始'.format(thread_name, count))
        response = requests.post(url, json=json_data, headers=headers)  # 发送数据
        # 检查响应状态码
        if response.status_code == 200:
            # 如果同时满足response.json()['Code']不等于100和response.json()['Message']为该配送单已经存在！的条件，则请求失败
            if (response.json()['Message'] == '服务端错误： 已添加了具有相同键的项。' or response.json()['Message'] == '服务端错误： 未将对象引用设置到对象的实例。'
                    or response.json()['Message'] == '您的登录已过期，请重新登录' or response.json()['Message'] == '服务端错误： 索引超出了数组界限。'):
                write_log.write_log('{}: 第{}个订单需要重传，{}'.format(thread_name, count, response.json()['Message']))
                write_log.write_log(json.dumps(json_data, ensure_ascii=False, indent=2))
                return False
            elif response.json()['Code'] != 100 and response.json()['Message'] != '该配送单已经存在！':
                write_log.write_log('{}: 第{}个订单请求失败，{}'.format(thread_name, count, response.json()['Message']))
                write_log.write_log(json.dumps(json_data, ensure_ascii=False, indent=2))
                sql_str1 = """
                                UPDATE  wy_dist_to_mclz
                                set state ='3',remark='{}',finish_time=now()
                                where  bill_number='{}'
                                """.format(response.json()['Message'], billnumber)
                # 连接数据库
                cursor1 = dbcon.exec_sql(cnx, sql_str1)
                if cursor1 is None:
                    dbcon.db_disconnect(cnx)  # 关闭数据库连接
                    return False
                sql_str2 = """
                                UPDATE  wy_dist_to_mclz_log
                                set state ='3',remark='{}',finish_time=now()
                                where  bill_number='{}'
                                """.format(response.json()['Message'], billnumber)
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
            else:
                # 请求成功
                sql_str1 = """
                UPDATE  wy_dist_to_mclz
                set state ='1',remark='更新成功',finish_time=now()
                where  bill_number='{}'
                """.format(billnumber)
                # 连接数据库
                cursor1 = dbcon.exec_sql(cnx, sql_str1)
                if cursor1 is None:
                    dbcon.db_disconnect(cnx)  # 关闭数据库连接
                    return False
                sql_str2 = """
                UPDATE  wy_dist_to_mclz_log
                set state ='1',remark='更新成功',finish_time=now()
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
        else:
            dbcon.db_disconnect(cnx)  # 关闭数据库连接
            write_log.write_log(f"Request failed with status code: {response.status_code}")
            write_log.write_log("Response:")
            write_log.write_log(response.text)
            write_log.write_log(json.dumps(json_data, ensure_ascii=False, indent=2))
    except Exception as e:
        dbcon.db_disconnect(cnx)  # 关闭数据库连接
        write_log.write_log(f"An error occurred: {str(e)}")
        write_log.write_log(json.dumps(json_data, ensure_ascii=False, indent=2))


def update_table(table_name, state, remark, billnumber):
    cnx = dbcon.db_connect()  # 连接数据库
    if cnx is None:
        write_log.write_log('{}: update{}连接数据库失败'.format(table_name, billnumber))
        return False
    sql_str = """
    UPDATE  {}
    set state ='{}',remark='{}',finish_time=now()
    where  bill_number='{}'
    """.format(table_name, state, remark, billnumber)
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


if __name__ == '__main__':
    write_log.write_log('开始')
    write_log.write_log('结束')
