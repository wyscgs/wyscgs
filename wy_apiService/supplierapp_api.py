# 提供接口给蔬菜公司系统调用
# by ly 20240113
import dbconnect
import write_log
import json
from datetime import datetime

# 定义每页行数
per_page = 50
statement_page = 100


# 判断日期格式函数
def is_datetime(datetime_string):
    try:
        datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False


# 根据日期及供应商号获取订单号列表
# by ly 20240113
def get_purchase_billnumber_list(b_date, e_date, supplier_codes):
    if not is_datetime(b_date):
        write_log.write_log("supplierapp_api.get_purchase_billnumber_list:开始日期格式错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误A'})
    if not is_datetime(e_date):
        write_log.write_log("supplierapp_api.get_purchase_billnumber_list:结束日期格式错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误B'})
    if supplier_codes is None:
        write_log.write_log("supplierapp_api.get_purchase_billnumber_list:供应商号列表错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误C'})
    sql_query = f"SELECT bill_number,supplier_code,date_format(delivery_time,'%Y-%m-%d %H:%i:%s') as delivery_time FROM e_purchase_order WHERE supplier_code IN ({supplier_codes}) AND delivery_time >= '{b_date}' AND delivery_time <= '{e_date}'"
    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_purchase_billnumber_list:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_purchase_billnumber_list:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_purchase_billnumber_list:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_billnumber_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["bill_number", "supplier_code", "delivery_time"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['bill'] = cum_list
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_purchase_billnumber_list:返回{result_dic}")
    return result_dic


# 根据供应商订单单据号获取订单明细
# by ly 20240113
def get_purchase_detail(bill_number):
    sql_query = (
        f"SELECT  uuid, version, date_format(create_time,'%Y-%m-%d %H:%i:%s') as create_time, create_oper_code, create_oper_name,"
        f"  date_format(last_modify_time,'%Y-%m-%d %H:%i:%s') as last_modify_time, last_modify_oper_code, "
        f"  last_modify_oper_name, address, bill_number, date_format(check_time,'%Y-%m-%d %H:%i:%s') as check_time, check_oper_code, check_oper_name, close as is_close, confirm_time, "
        f"  confirm_oper_code, confirm_oper_name, contact, contact_mobile, date_format(delivery_time,'%Y-%m-%d %H:%i:%s') as delivery_time, "
        f"  date_format(dist_date,'%Y-%m-%d %H:%i:%s') as dist_date, is_direct_delivery, "
        f"  is_pre, pre_confirm_time, pre_confirm_oper_code, pre_confirm_oper_name, pre_supplier_confirm, remark, school_code, "
        f"  school_name, source_bill_number, source_bill_type, source_bill_uuid, state, supplier_code, supplier_confirm, supplier_name,"
        f"  total_count, total_money, type, warehouse_code, warehouse_name, arrival_time, is_system_auto_push, mode, statement_bill_confirm"
        f" FROM e_purchase_order "
        f"WHERE  bill_number = '{bill_number}' ")
    sql_query_line = (
        f" SELECT uuid, version, line_number, money, plan_quantity, price, product_code, product_name, product_spec,"
        f" product_unit, purchase_bill_number, quantity, received_gift_quantity, received_quantity, tax,"
        f"  tax_rate, tax_type, sale_price, remark, audit_quantity, received_several, sale_amount, several "
        f" FROM e_purchase_order_line"
        f"  WHERE  purchase_bill_number = '{bill_number}' ")
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_purchase_detail:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    connector_line = connect_obj.cursor()
    # 执行查询单据主信息的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_purchase_detail:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_purchase_detail:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_detail:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})

    # 执行查询单据明细语句
    try:
        connector_line.execute(sql_query_line)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_purchase_detail:sql：{sql_query_line}。")
        write_log.write_log(f"supplierapp_api.get_purchase_detail:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})

    # 获取返回行
    results_line = connector_line.fetchall()
    rows_count = len(results_line)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_detail:无数据")
        return json.dumps({"status": 0, "msg": "无明细数据！"})

    # 处理主信息
    cum_name = ["uuid", "version", "create_time", "create_oper_code", "create_oper_name", "last_modify_time",
                "last_modify_oper_code",
                "last_modify_oper_name", "address", "bill_number", "check_time", "check_oper_code", "check_oper_name",
                "is_close", "confirm_time",
                "confirm_oper_code", "confirm_oper_name", "contact", "contact_mobile", "delivery_time", "dist_date",
                "is_direct_delivery",
                "is_pre", "pre_confirm_time", "pre_confirm_oper_code", "pre_confirm_oper_name", "pre_supplier_confirm",
                "remark", "school_code",
                "school_name", "source_bill_number", "source_bill_type", "source_bill_uuid", "state", "supplier_code",
                "supplier_confirm", "supplier_name",
                "total_count", "total_money", "type", "warehouse_code", "warehouse_name", "arrival_time",
                "is_system_auto_push", "mode", "statement_bill_confirm"]
    result_dic = {'status': 1, 'msg': '查询成功'}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['bill'] = cum_list

    # 处理明细信息
    order_line = ["uuid", "version", "line_number", "money", "plan_quantity", "price", "product_code", "product_name",
                  "product_spec",
                  "product_unit", "purchase_bill_number", "quantity", "received_gift_quantity", "received_quantity",
                  "tax",
                  "tax_rate", "tax_type", "sale_price", "remark", "audit_quantity", "received_several", "sale_amount",
                  "several"]
    order_list = []
    for row in results_line:
        temp = {}
        for i in range(len(row)):
            temp[order_line[i]] = row[i]
        order_list.append(temp)
    result_dic['bill_line'] = order_list

    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_purchase_detail:返回{result_dic}")
    return result_dic


# 根据供应号获取进货单和退货单单据列表
# by ly 20240114
def get_receipt_billnumber_list(supplier_codes):
    sql_query = f"update wy_trans_to_suppsystem set state = 2 WHERE supplier_code IN ({supplier_codes}) and state = 0 "
    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_receipt_billnumber_list:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    try:
        connector.execute(sql_query)
        # 提交更改
        connect_obj.commit()
    except connect_obj.connector.Error as err:
        dbconnect.db_disconnect(connect_obj)
        # 如果更新失败，打印数据库错误信息
        write_log.write_log(f"supplierapp_api.get_receipt_billnumber_list:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})

    sql_query = f"select bill_number,bill_type from  wy_trans_to_suppsystem where state = 2 and supplier_code IN ({supplier_codes}) group by bill_number,bill_type"
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_receipt_billnumber_list:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_receipt_billnumber_list:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_receipt_billnumber_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["bill_number", "bill_type"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['bill'] = cum_list

    sql_query = f"update wy_trans_to_suppsystem set state = 1,finish_time = sysdate() where state = 2 and supplier_code IN ({supplier_codes}) "
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
        # 提交更改
        connect_obj.commit()
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_receipt_billnumber_list:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_receipt_billnumber_list:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})

    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_receipt_billnumber_list:返回{result_dic}")
    return result_dic


# 根据采购入库单单据号获取采购入库明细
# by ly 20240115
def get_receipt_detail(bill_number):
    sql_query = (
        f"select bill_number as docno,date_format(create_time,'%Y-%m-%d %H:%i:%s') as docdat,create_oper_name as docname,'批发' as doctype,"
        f"date_format(receipt_time,'%Y-%m-%d %H:%i:%s') as vailddat,last_modify_oper_name as vaildname, case when state = 'received' then '1' else '2' end as vaild,"
        f"warehouse_code as cumno,'0604' as paytype,    date_format(receipt_time,'%Y-%m-%d %H:%i:%s') as paydate,"
        f" '' as ywname,0 as csamt,0 as syamt,0 as qty,100 as disc,'系统同步数据' as remark,"
        f" 'YK0101' AS  storeno,supplier_code,supplier_name,purchase_bill_number,state,date_format(DATE_ADD(plan_receipt_date,interval 1 day),'%Y-%m-%d %H:%i:%s') as dist_date "
        f" from e_receipt  "
        f"WHERE  bill_number = '{bill_number}' ")
    sql_query_line = (
        f" select receipt_bill_number  as docno,line_number as sno,product_code,"
        f" quantity as qty,price as slprc,money as slamt,date_format(production_date,'%Y-%m-%d %H:%i:%s') as scdat,batch as pcno ,state as line_state,tax_rate"
        f" from e_receipt_line"
        f"  WHERE  receipt_bill_number = '{bill_number}' ")
    print(sql_query_line)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_receipt_detail:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    connector_line = connect_obj.cursor()
    # 执行查询单据主信息的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_receipt_detail:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_receipt_detail:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchall()
    # print(results)
    rows_count = len(results)
    # print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_receipt_detail:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})
    # 执行查询单据明细语句
    try:
        connector_line.execute(sql_query_line)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_receipt_detail:sql：{sql_query_line}。")
        write_log.write_log(f"supplierapp_api.get_receipt_detail:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})

    # 获取返回行
    results_line = connector_line.fetchall()
    rows_count = len(results_line)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_receipt_detail:无数据")
        return json.dumps({"status": 0, "msg": "无明细数据！"})

    # 处理主信息
    cum_name = ["docno", "docdat", "docname", "doctype", "vailddat", "vaildname", "vaild",
                "cumno", "paytype", "paydate", "ywname", "csamt", "syamt", "qty", "disc",
                "remark", "storeno", "supplier_code", "supplier_name", "purchase_bill_number", "state", "dist_date"]
    result_dic = {'status': 1, 'msg': '查询成功'}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['bill'] = cum_list

    # 处理明细信息
    order_line = ["docno", "sno", "product_code", "qty", "slprc", "slamt", "scdat", "pcno", "line_state", "tax_rate"]
    order_list = []
    for row in results_line:
        temp = {}
        for i in range(len(row)):
            temp[order_line[i]] = row[i]
        order_list.append(temp)
    result_dic['bill_line'] = order_list

    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_receipt_detail:返回{result_dic}")
    return result_dic


# 根据供应商号获取采购目录信息，分页显示
# by ly 20240115
def get_product_list(supplier_codes, page_num):
    start_index = (page_num - 1) * per_page
    if supplier_codes is None:
        write_log.write_log("supplierapp_api.get_product_list:供应商号列表错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误C'})
    sql_query = (
        f" select c.code,c.name,c.specification as spec,c.unit,b.supplier_code,b.supplier_name,b.purchase_price ,c.tax_revenue_code,c.output_tax_rate * 100 as tax_rate,"
        f"c.three_category_code,c.three_category_name,c.net_weight ,d.remark as cw_category"
        f" from e_purchase_catalog a,e_purchase_catalog_line b,e_product c left outer join e_product_category d on c.three_category_code = d.code"
        f" where a.uuid = b.purchase_catalog_uuid and b.product_code = c.code "
        # f" and begin_date >=date_Add(sysdate(),interval -60 day) "
        f" and ( (begin_date <=date_Add(sysdate(),interval 10 day) and date_format(end_date,'%Y%m%d') >=date_format(date_Add(sysdate(),interval 10 day),'%Y%m%d'))"
        f" or  ( begin_date <=date_Add(sysdate(),interval 3 day) and date_format(end_date,'%Y%m%d') >=date_format(date_Add(sysdate(),interval 3 day),'%Y%m%d')))"
        f" and supplier_code in ({supplier_codes}) "
        f" group by  c.code,c.name,c.specification  ,c.unit,b.supplier_code,b.supplier_name,b.purchase_price,c.tax_revenue_code,c.output_tax_rate,"
        f"c.three_category_code,c.three_category_name,c.net_weight,d.remark"
        f" order by supplier_code,product_code limit {start_index},{per_page} ")
    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_product_list:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_product_list:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_product_list:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_product_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["code", "name", "spec", "unit", "supplier_code", "supplier_name", "purchase_price", "tax_revenue_code",
                "tax_rate", "three_category_code", "three_category_name", "net_weight","cw_category"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['product'] = cum_list
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_product_list:返回{result_dic}")
    return result_dic


# 根据采购退货单单据号获取采购退货明细
# by ly 20240117
def get_purchase_return_detail(bill_number):
    sql_query = (
        f"select a.bill_number as docno,DATE_FORMAT(create_time,'%Y-%m-%d %H:%i:%s') as docdat,"
        f"return_oper_name as docname,DATE_FORMAT(occur_date,'%Y-%m-%d %H:%i:%s') as vailddat,return_oper_name as vaildname,"
        f"'1' as vaild,warehouse_code as cumno,'0604' AS paytype,DATE_FORMAT(occur_date,'%Y-%m-%d %H:%i:%s') as paydate,"
        f"'系统管理' as ywname,0 as csamt,0 as syamt,0 as qty ,'系统导入' as remark,'YK0101' as storeno,"
        f" supplier_code,supplier_name,state,DATE_FORMAT(ifnull(b.dist_Date,occur_date),'%Y-%m-%d %H:%i:%s') as dist_Date "
        f" from e_purchase_return a "
        f" left outer join  (select bill_number,dist_Date from e_purchase_order) b on a.purchase_bill_number = b.bill_number"
        f" WHERE  a.bill_number = '{bill_number}' ")
    sql_query_line = (
        f" select return_bill_number  as docno,line_number as sno,product_code,"
        f" 'YK0101' as storeno,quantity as qty,price as slprc,amount as slamt,tax_rate * 100 as outtx"
        f" from e_purchase_return_line "
        f"  WHERE  return_bill_number = '{bill_number}' ")
    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_purchase_return_detail:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    connector_line = connect_obj.cursor()
    # 执行查询单据主信息的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_purchase_return_detail:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_purchase_return_detail:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchall()
    # print(results)
    rows_count = len(results)
    # print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_return_detail:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})
    # 执行查询单据明细语句
    try:
        connector_line.execute(sql_query_line)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_purchase_return_detail:sql：{sql_query_line}。")
        write_log.write_log(f"supplierapp_api.get_purchase_return_detail:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})

    # 获取返回行
    results_line = connector_line.fetchall()
    rows_count = len(results_line)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_return_detail:无数据")
        return json.dumps({"status": 0, "msg": "无明细数据！"})

    # 处理主信息
    cum_name = ["docno", "docdat", "docname", "vailddat", "vaildname", "vaild", "cumno", "paytype", "paydate",
                "ywname", "csamt", "syamt", "qty", "remark", "storeno", "supplier_code", "supplier_name", "state",
                "dist_date"]
    result_dic = {'status': 1, 'msg': '查询成功'}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['bill'] = cum_list

    # 处理明细信息
    order_line = ["docno", "sno", "product_code", "storeno", "qty", "slprc", "slamt", "outtx"]
    order_list = []
    for row in results_line:
        temp = {}
        for i in range(len(row)):
            temp[order_line[i]] = row[i]
        order_list.append(temp)
    result_dic['bill_line'] = order_list

    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_purchase_return_detail:返回{result_dic}")
    return result_dic


# 根据仓库信息获取生成并获取批次号
# by lb 20240219
# 获取当前日期的批次号顺序号
def get_batch_sequence_number():
    # 根据日期获取批次号的顺序号
    sql_query = "select param_1,param_2 from wy_system_config where o_key = 'batchsequencenumber'"
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_batch_sequence_number:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询根据日期获取批次号的顺序号
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_batch_sequence_number:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_batch_sequence_number:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchone()
    rows_count = len(results)
    # print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_batch_sequence_number:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})
    param_1 = results[0]
    param_2 = results[1]
    print(param_1, param_2)
    current_date = datetime.now().strftime('%Y-%m-%d')
    print(current_date)
    if param_2 != current_date:
        # 更新param_2为当前日期，param_1为999999
        sql_update = "UPDATE wy_system_config SET param_1 = '999999', param_2 = %s WHERE o_key = 'batchsequencenumber'"
        try:
            connector.execute(sql_update, (current_date,))
            connect_obj.commit()
            batch_sequence_number = '999999'
        except connect_obj.Error as err:
            dbconnect.db_disconnect(connect_obj)
            print(f'执行SQL语句失败，联系系统管理员{err}')
            write_log.write_log(f"supplierapp_api.get_batch_sequence_number:sql：{sql_update}。")
            write_log.write_log(f"supplierapp_api.get_batch_sequence_number:执行语句失败：{err}。")
            return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    else:
        # 更新param_1的值为转化为数字并且减1
        new_param_1 = str(int(param_1) - 1)
        sql_update = "UPDATE wy_system_config SET param_1 = %s WHERE o_key = 'batchsequencenumber'"
        try:
            connector.execute(sql_update, (new_param_1,))
            connect_obj.commit()
            batch_sequence_number = new_param_1.zfill(6)
        except connect_obj.Error as err:
            dbconnect.db_disconnect(connect_obj)
            print(f'执行SQL语句失败，联系系统管理员{err}')
            write_log.write_log(f"supplierapp_api.get_batch_sequence_number:sql：{sql_update}。")
            write_log.write_log(f"supplierapp_api.get_batch_sequence_number:执行语句失败：{err}。")
            return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    return batch_sequence_number


# 根据获得的顺序号生成批次号
def generate_batch_number():
    current_date = datetime.now().strftime('%Y%m%d')
    sequence_number = get_batch_sequence_number()
    batch_number = current_date + sequence_number
    return batch_number


# 将生成的批次号插入到到e_batch中
def insert_batch(wrh_code, wrh_name, batch):
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.insert_batch:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 插入批次号到e_batch中
    sql_insert = """
    insert into e_batch (uuid,version,create_time,create_oper_code,create_oper_name,
    last_modify_time,last_modify_oper_code,last_modify_oper_name,batch,wrh_code,wrh_name)
    VALUES 
    (uuid(),'0',NOW(),%s,%s,NOW(),%s,%s,%s,%s,%s)
    """
    try:
        connector.execute(sql_insert, (wrh_code, wrh_name, wrh_code, wrh_name, batch, wrh_code, wrh_name))
        connect_obj.commit()

    except connect_obj.Error as err:
        connector.close()
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.insert_batch:sql：{sql_insert}。")
        write_log.write_log(f"supplierapp_api.insert_batch:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    finally:
        connector.close()
        dbconnect.db_disconnect(connect_obj)


# 判断post的仓库编码和名称是否存在
def warehouse_exists(wrh_code, wrh_name):
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_batch_sequence_number:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 判断post的仓库编码和名称是否存在
    sql_query = "SELECT COUNT(*) as count FROM e_warehouse WHERE code = %s AND name = %s"
    # 执行查询根据日期获取批次号的顺序号
    try:
        connector.execute(sql_query, (wrh_code, wrh_name))
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_batch_sequence_number:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_batch_sequence_number:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    results = connector.fetchone()
    rows_count = results[0]
    # print("rows_count=", rows_count)
    if rows_count > 0:
        dbconnect.db_disconnect(connect_obj)
        return True
    dbconnect.db_disconnect(connect_obj)
    return False


# 通过输入仓库编码和仓库名称生成批次号并插入batch表中
def get_batch(wrh_code, wrh_name):
    if not warehouse_exists(wrh_code, wrh_name):
        write_log.write_log(f"supplierapp_api.get_batch:输入的仓库编码和仓库名称有误。")
        return json.dumps({'status': 0,
                           'meg': f'输入的仓库编码wrh_code：{wrh_code} 和仓库名称wrh_name：{wrh_name} 有误，请检查后重新输入！'})
    batch = generate_batch_number()
    insert_batch(wrh_code, wrh_name, batch)
    return json.dumps({'status': 1, 'meg': '生成批次号成功！', 'batch': batch})


# 根据lineuuid获取获取采购订单详情行
# by lb 20240221
def get_purchase_order_line_details(lineuuid):
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_batch_sequence_number:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    sql_query = "select line,batch,CONVERT(quantity, FLOAT) quantity  from  e_purchase_order_line_details  WHERE purchase_line_uuid = %s"
    try:
        connector.execute(sql_query, (lineuuid,))
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_batch_sequence_number:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_batch_sequence_number:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchall()
    # 获取列名
    cum_name = [description[0] for description in connector.description]
    rows_count = len(results)
    # print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_order_line_details:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})
    result_dic = {'status': 1, 'msg': '查询成功'}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['line_details'] = cum_list
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_purchase_order_line_details:返回{result_dic}")
    return result_dic


# 海鼎数据接口 提供给供应商系统调用,提供当期的采购商品的价格
# by lb 20240327
def get_purchase_catalog_price(product_code, warehouse_code, purchase_date, supplier_code):
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_purchase_catalog_price:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    sql_query = """
    SELECT   CONVERT(b.purchase_price,FLOAT) purchase_price,b.product_code,b.product_name,a.code  catalog_code,a.name  catalog_name,
    DATE_FORMAT(a.begin_date, '%Y-%m-%d') begin_date,DATE_FORMAT(a.end_date, '%Y-%m-%d')  end_date,
    b.supplier_code,b.supplier_name,b.warehouse_code,b.warehouse_name
    from    e_purchase_catalog a,e_purchase_catalog_line b
    where  a.uuid=b.purchase_catalog_uuid
    and  b.product_code=%s
    and  b.warehouse_code=%s
    and  b.supplier_code=%s
    and  a.begin_date<=%s
    and  a.end_date>=%s
    """
    try:
        connector.execute(sql_query, (product_code, warehouse_code, supplier_code, purchase_date, purchase_date))
        # connector.execute(sql_query)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_purchase_catalog_price:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_purchase_catalog_price:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchall()
    # 获取列名
    cum_name = [description[0] for description in connector.description]
    rows_count = len(results)
    # print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_order_line_details:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})
    result_dic = {'status': 1, 'msg': '查询成功'}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['product_info'] = cum_list
    result_dic['price'] = cum_list[0]["purchase_price"]
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_purchase_catalog_price:返回{result_dic}")
    return result_dic


# 根据purchase_bill_number和插入和作废订单获取获取采购订单详情行
# by lb 20240327
def insert_purchase_order(purchase_order_info):
    total_count = 0.00
    total_money = 0.00
    old_bill_number = purchase_order_info["old_bill_number"]
    new_bill_number = purchase_order_info["new_bill_number"]
    new_supplier_code = purchase_order_info["new_supplier_code"]
    new_supplier_name = get_supplier_name(new_supplier_code)
    # write_log.write_log(new_supplier_name)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.insert_purchase_order:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})

    connector = connect_obj.cursor()
    # 提取列表中的值并插入到 lb_temp_purchase_order_line_temp20240401 表中
    for item in purchase_order_info["list"]:
        line = item["line"]
        product_code = item["product_code"]
        quantity = item["quantity"]
        price = item["price"]
        money = float(quantity) * float(price)
        # 获取细项的总金额及总数量
        total_count = total_count + float(quantity)
        total_money = total_money + money
        sql_purchase_line = """
            insert into e_purchase_order_line  
            (uuid,version,line_number,money,plan_quantity,
            price,product_code,product_name,product_spec,product_unit,purchase_bill_number,quantity,received_gift_quantity,
            received_quantity,tax,tax_rate,tax_type,sale_price,remark,audit_quantity,received_several,sale_amount,several
            )
            select uuid(),version,'{}','{}','{}','{}','{}',product_name,product_spec,product_unit,'{}','{}',
            received_gift_quantity,'{}',{}/(1+tax_rate)*tax_rate ,tax_rate,tax_type,sale_price,remark,'{}',
            received_several,sale_amount,several
            from e_purchase_order_line  
            where purchase_bill_number='{}'
            and product_code='{}'
        """.format(line, money, quantity, price, product_code, new_bill_number, quantity, quantity, money, quantity,
                   old_bill_number, product_code)
        write_log.write_log(sql_purchase_line)
        try:
            connector.execute(sql_purchase_line)
        except connect_obj.Error as err:
            connector.close()
            dbconnect.db_disconnect(connect_obj)
            write_log.write_log(f"supplierapp_api.insert_purchase_order:sql：{sql_purchase_line}。")
            write_log.write_log(f"supplierapp_api.insert_purchase_order:执行语句失败：{err}。")
            return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 插入e_purchase_order主表
    sql_purchase = """
    insert into e_purchase_order
    (
        uuid,version,create_time,create_oper_code,create_oper_name,last_modify_time,last_modify_oper_code,
        last_modify_oper_name,address,bill_number,check_time,check_oper_code,check_oper_name,close,
        confirm_time,confirm_oper_code,confirm_oper_name,contact,contact_mobile,delivery_time,dist_date,
        is_direct_delivery,is_pre,pre_confirm_time,pre_confirm_oper_code,	pre_confirm_oper_name,
        pre_supplier_confirm,remark,school_code,school_name,source_bill_number,source_bill_type,source_bill_uuid,
        state,supplier_code,supplier_confirm,supplier_name,total_count,total_money,type,warehouse_code,warehouse_name,
        arrival_time,is_system_auto_push,mode,statement_bill_confirm
    )
    select uuid(),version,NOW(),create_oper_code,create_oper_name,last_modify_time,last_modify_oper_code,
    last_modify_oper_name,address,'{}',check_time,check_oper_code,check_oper_name,close,
    confirm_time,confirm_oper_code,confirm_oper_name,contact,contact_mobile,delivery_time,dist_date,
    is_direct_delivery,is_pre,pre_confirm_time,pre_confirm_oper_code,	pre_confirm_oper_name,
    pre_supplier_confirm,remark,school_code,school_name,'{}',source_bill_type,source_bill_uuid,
    'checked','{}',supplier_confirm,'{}',{},{},type,warehouse_code,warehouse_name,
    arrival_time,is_system_auto_push,mode,statement_bill_confirm
    from e_purchase_order 
    where  bill_number='{}'
    """.format(new_bill_number, old_bill_number, new_supplier_code, new_supplier_name, total_count, total_money,
               old_bill_number)
    # 定义参数值
    write_log.write_log(sql_purchase)
    try:
        connector.execute(sql_purchase)
        connect_obj.commit()
    except connect_obj.Error as err:
        connector.close()
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log(f"supplierapp_api.insert_purchase_order:sql：{sql_purchase}。")
        write_log.write_log(f"supplierapp_api.insert_purchase_order:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    connector.close()
    dbconnect.db_disconnect(connect_obj)
    result_dic = {'status': 1, 'msg': '插入成功'}
    write_log.write_log(f"supplierapp_api.sql_purchase:返回{purchase_order_info}")
    return result_dic


# 根据供应商编号获取供应商名称
def get_supplier_name(new_supplier_code):
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_batch_sequence_number:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    sql_query = "select   name from  e_supplier where  code='{}'".format(new_supplier_code)
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log(f"supplierapp_api.get_supplier_name:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_supplier_name:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchone()
    rows_count = len(results)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_supplier_name:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})
    supplier_name = results[0]
    connector.close()
    dbconnect.db_disconnect(connect_obj)
    return supplier_name


def aborted_purchase_order(purchase_order_info):
    old_bill_number = purchase_order_info["old_bill_number"]
    last_modify_oper_code = purchase_order_info["last_modify_oper_code"]
    last_modify_oper_name = purchase_order_info["last_modify_oper_name"]
    remark = purchase_order_info["remark"]

    state = get_purchase_order_state(old_bill_number)
    if state == "aborted":
        return json.dumps({'status': -100, 'msg': '当前订单状态为作废，不允许作废！'})
    elif state == "partialReceived":
        return json.dumps({'status': -100, 'msg': '当前订单状态为部分收货，不允许作废！'})
    elif state == "finished":
        return json.dumps({'status': -100, 'msg': '当前订单状态为已完成，不允许作废！'})
    else:
        # 连接数据库
        connect_obj = dbconnect.db_connect()
        if connect_obj is None:
            write_log.write_log("supplierapp_api.aborted_purchase_order:连接数据库失败")
            return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
        connector = connect_obj.cursor()
        # 将e_purchase_order主表中old_bill_number状态置为作废
        sql_update_purchase = """
            update e_purchase_order
            set state='aborted',last_modify_oper_code='{}',last_modify_oper_name='{}',remark='{}',last_modify_time=NOW()
            where   bill_number='{}'
            """.format(last_modify_oper_code, last_modify_oper_name, remark, old_bill_number)
        # 定义参数值
        write_log.write_log(sql_update_purchase)
        try:
            connector.execute(sql_update_purchase)
            connect_obj.commit()
        except connect_obj.Error as err:
            connector.close()
            dbconnect.db_disconnect(connect_obj)
            write_log.write_log(f"supplierapp_api.aborted_purchase_order:sql：{sql_update_purchase}。")
            write_log.write_log(f"supplierapp_api.aborted_purchase_order:执行语句失败：{err}。")
            return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
        connector.close()
        dbconnect.db_disconnect(connect_obj)
        result_dic = {'status': 1, 'msg': '作废成功'}
        write_log.write_log(f"supplierapp_api.aborted_purchase_order:返回{old_bill_number}")
        return result_dic


# 获取purchase_order的订单状态
def get_purchase_order_state(bill_number):
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_purchase_order_state:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 判断post的仓库编码和名称是否存在
    sql_query = "select   state  from e_purchase_order  where bill_number ='{}' ".format(bill_number)
    # 执行查询根据日期获取purchase_order的订单号获取状态
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        connector.close()
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log(f"supplierapp_api.get_purchase_order_state:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_purchase_order_state:执行语句失败：{err}。")
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取单据主信息返回行
    results = connector.fetchone()
    rows_count = len(results)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_purchase_order_state:无数据")
        return json.dumps({"status": 0, "msg": "无单据数据！"})
    state = results[0]
    connector.close()
    dbconnect.db_disconnect(connect_obj)
    return state


# 根据供应商号及日期获取供应商对账单信息，分页显示
# by ly 20240401
def get_supplier_statement(bdate, supplier_codes, page_num):
    start_index = (page_num - 1) * statement_page
    if supplier_codes is None:
        write_log.write_log("supplierapp_api.get_supplier_statement:供应商号列表错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误C'})
    sql_query = (
        f" select a.supplier_code,a.bill_number,b.account_bill_number, DATE_FORMAT( a.rec_start_date, '%Y-%m-%d')   as rec_start_date,  "
        f" DATE_FORMAT( a.rec_end_date, '%Y-%m-%d')  as rec_end_date,   DATE_FORMAT( max(b.occur_date), '%Y-%m-%d')   as occur_date,"
        f" b.bill_type,b.warehouse_code,sum(b.amount) as amount   "
        f" from e_supplier_statement a,e_supplier_statement_detail b where a.bill_number = b.statement_bill_number   "
        f" and a.state <> 'deleted' and a.rec_start_date <='{bdate}' and a.rec_end_date >='{bdate}'  and a.supplier_code in ({supplier_codes})  "
        f" group by a.supplier_code,a.bill_number,b.account_bill_number,a.rec_start_date,a.rec_end_date,b.bill_type,b.warehouse_code  "
        f" order by a.supplier_code,a.bill_number,b.account_bill_number,a.rec_start_date,a.rec_end_date,b.bill_type,b.warehouse_code  "
        f" limit {start_index},{statement_page} ")
    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_supplier_statement:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # print(sql_query)
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_supplier_statement:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_supplier_statement:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_supplier_statement:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["supplier_code", "bill_number", "account_bill_number", "rec_start_date", "rec_end_date", "occur_date",
                "bill_type", "warehouse_code", "amount"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['list'] = cum_list
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_supplier_statement:返回{result_dic}")
    return result_dic


# 根据供应商号获取采购目录信息，分页显示
# by ly 20240115
def get_product_bycode(product_code):
    if product_code is None:
        write_log.write_log("supplierapp_api.get_product_bycode:商品编码错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误C'})
    sql_query = (
        f" select c.code,c.name,c.specification as spec,c.unit,'' as supplier_code,'' as supplier_name,0 as purchase_price ,"
        f"c.tax_revenue_code,c.output_tax_rate * 100 as tax_rate,c.three_category_code,c.three_category_name,net_weight,d.remark as cw_category "
        f" from e_product c left outer join e_product_category d on  c.three_category_code = d.code where c.code =  '{product_code}' ")
    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_product_bycode:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_product_bycode:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_product_bycode:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_product_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["code", "name", "spec", "unit", "supplier_code", "supplier_name", "purchase_price", "tax_revenue_code",
                "tax_rate", "three_category_code", "three_category_name", "net_weight","cw_category"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['product'] = cum_list
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_product_bycode:返回{result_dic}")
    return result_dic


# 根据日期及供应商号获取所有单据信息
# by ly 20240529
def get_bill_list(b_date, e_date, supplier_codes, page_num):
    if not is_datetime(b_date):
        print(b_date)
        write_log.write_log("supplierapp_api.get_bill_list:开始日期格式错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误A'})
    if not is_datetime(e_date):
        print(b_date)
        write_log.write_log("supplierapp_api.get_bill_list:结束日期格式错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误B'})
    if supplier_codes is None:
        write_log.write_log("supplierapp_api.get_bill_list:供应商号列表错误")
        return json.dumps({'status': -100, 'meg': '输入参数格式错误C'})
    start_index = (page_num - 1) * statement_page

    sql_query = (
        f" select bill_number,warehouse_code,supplier_code,supplier_name,    DATE_FORMAT( DATE_ADD(plan_receipt_date,INTERVAL 1 day) , '%Y-%m-%d')  as dist_date,"
        f" DATE_FORMAT(     receipt_time, '%Y-%m-%d') as receipt_time,sum(b.quantity) as qty,sum(money) as csamt,'receipt' as doctype "
        f"    from e_receipt  a,e_receipt_line b "
        f" where a.bill_number = b.receipt_bill_number and a.state = 'received' and b.state <> 'aborted' "
        f"and  a.receipt_time >='{b_date}' and a.receipt_time <='{e_date}'"
        #  f"and  DATE_ADD(plan_receipt_date,INTERVAL 1 day) >='{b_date}' and DATE_ADD(plan_receipt_date,INTERVAL 1 day) <='{e_date}'"
        f"and supplier_code IN ({supplier_codes}) "
        f"group by bill_number,warehouse_code,supplier_code,supplier_name,DATE_ADD(plan_receipt_date,INTERVAL 1 day)  "
        f"union all "
        f"select bill_number,warehouse_code,supplier_code,supplier_name,DATE_FORMAT(occur_DATE , '%Y-%m-%d') ,DATE_FORMAT(RETURN_OPER_TIME , '%Y-%m-%d') , case when a.state = 'aborted' then 0 else -sum(quantity)  end as qty, "
        f"case when a.state = 'aborted' then 0 else -sum(amount) end as csamt ,'return' as doctype"
        f" from e_purchase_return  a,e_purchase_return_line b "
        f" where a.bill_number = b.return_bill_number and a.state IN ( 'finished' ,'aborted')  "
        f"and  occur_DATE >='{b_date}' and occur_DATE <='{e_date}' "
        f"and supplier_code IN ({supplier_codes})"
        f"group by bill_number,warehouse_code,supplier_code,supplier_name,occur_DATE,RETURN_OPER_TIME "
        f"order by bill_number limit {start_index},{statement_page}")

    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_bill_list:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_bill_list:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_bill_list:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_bill_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["bill_number", "warehouse_code", "supplier_code", "supplier_name", "dist_date", "receipt_time", "qty",
                "csamt", 'doctype']
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['bill'] = cum_list
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_bill_list:返回{result_dic}")
    return result_dic


# 根据查询时间段内对应供应商号 采购目录的所有有效商品
# by lb 20240710
def get_supplier_catalog_product(begin_date,end_date,supplier_code):

    sql_query = f"""
    select    DISTINCT b.product_code,b.product_name, b.supplier_code,b.supplier_name
    from  e_purchase_catalog a,e_purchase_catalog_line  b
    where a.uuid=b.purchase_catalog_uuid
    and  b.supplier_code='{supplier_code}'
    and a.begin_date<='{begin_date}'
    and a.end_date>='{end_date}'
    """
    print(sql_query)
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("supplierapp_api.get_supplier_catalog_product:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        print(f'执行SQL语句失败，联系系统管理员{err}')
        write_log.write_log(f"supplierapp_api.get_supplier_catalog_product:sql：{sql_query}。")
        write_log.write_log(f"supplierapp_api.get_supplier_catalog_product:执行语句失败：{err}。")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("supplierapp_api.get_supplier_catalog_product:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = [description[0] for description in connector.description]  # 获取列名
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['product'] = cum_list
    dbconnect.db_disconnect(connect_obj)
    write_log.write_log(f"supplierapp_api.get_supplier_catalog_product:返回{result_dic}")
    return result_dic
