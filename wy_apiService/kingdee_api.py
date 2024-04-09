# 金蝶调用的API
import dbconnect
import write_log
import json

# 定义每页行数
per_page = 50


# 获取供应商 或者学校信息
def get_customer_list(customer_type, page_num):
    start_index = (page_num - 1) * per_page
    sql_query = ""
    # 根据传入参数，判断是需要什么数据
    if customer_type.lower() == 'school':
        sql_query = """
                    select  b.code as code,b.name as name,a.name as  tax_name,
                    a.taxpayer_identification_number as tax_number ,'' as usc_code
                    from e_school b left outer join e_school_invoice_info a
                    on  a.school_code=b.code where  b.is_close = 0 order by b.code limit %s ,%s """
    elif customer_type.lower() == 'supplier':
        sql_query = """
                    select   code as code,name as name,name as tax_name,
                    '' as   tax_number ,uniform_social_code as usc_code  
                    from e_supplier order by code  limit %s ,%s """
    else:
        write_log.write_log("kingdee_api.get_customer_list:传入参数错误，请重新申请")
        return json.dumps({"status": -200, "meg": "传入参数错误，请重新申请"})

    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("kingdee_api.get_customer_list:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        n = connector.execute(sql_query, (start_index, per_page))
    except connect_obj.Error as err:
        print(f'连接数据库失败，联系系统管理员{err}')
        write_log.write_log(f"kingdee_api.get_customer_list:连接数据库失败{err}")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("kingdee_api.get_customer_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["code", "name", "tax_name", "tax_number", "usc_code"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count, 'customer_type': customer_type}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['info'] = cum_list
    dbconnect.db_disconnect(connector)
    write_log.write_log(f"kingdee_api.get_customer_list:返回{result_dic}")
    return result_dic


# 获取商品类别
def get_category_list(page_num):
    start_index = (page_num - 1) * per_page
    sql_query = """select code,name,remark,parent_code from  
                e_product_category order by code limit %s,%s """
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("kingdee_api.get_category_list:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        n = connector.execute(sql_query, (start_index, per_page))
    except connect_obj.Error as err:
        print(f'连接数据库失败，联系系统管理员{err}')
        write_log.write_log(f"kingdee_api.get_category_list:连接数据库失败{err}")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("kingdee_api.get_category_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["code", "name", "remark", "parent_code"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['info'] = cum_list
    dbconnect.db_disconnect(connector)
    write_log.write_log(f"kingdee_api.get_category_list:返回{result_dic}")
    return result_dic


# 获取商品基础信息
def get_product_list(code, page_num):
    start_index = (page_num - 1) * per_page
    sql_query = ""
    # 根据传入参数，判断是需要什么数据
    if code is not None:
        sql_query = """select a.code,a.name,ifnull(a.specification,'') as  specification,a.unit,
                    ifnull(a.spec_unit,'') as spec_unit,
                    '' as short_code,ifnull(a.introduction,'') as introduction,
                    '' as is_agricultural_product,ifnull(a.net_weight,0) as net_weight,
                    0 as product_width,0 as product_height,0 as product_length ,
                    '' as volume_unit, 0 as volume,a.one_category_code,a.one_category_name,
                    a.category_code,a.category_name,a.three_category_code,a.three_category_name,
                    b.remark,goods_category_code
                    from e_product  a,e_product_category b
                    where  a.category_code=b.code
                    and a.code = '{code}' 
                    and b.flevel ='two'
                    order by code limit %s,%s""".format(code=code)
    else:
        sql_query = """select a.code,a.name,ifnull(a.specification,'') as  specification,a.unit,
                    ifnull(a.spec_unit,'') as spec_unit,
                    '' as short_code,ifnull(a.introduction,'') as introduction,
                    '' as is_agricultural_product,ifnull(a.net_weight,0) as net_weight,
                    0 as product_width,0 as product_height,0 as product_length ,
                    '' as volume_unit, 0 as volume ,a.one_category_code,a.one_category_name,
                    a.category_code,a.category_name,a.three_category_code,a.three_category_name,
                    b.remark,goods_category_code
                    from e_product  a,e_product_category b
                    where a.category_code=b.code
                    and b.flevel ='two'
                    order by a.code limit %s,%s"""
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("kingdee_api.get_product_list:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    # 创建一个游标
    connector = connect_obj.cursor()
    # 执行查询出来的语句
    try:
        n = connector.execute(sql_query, (start_index, per_page))
    except connect_obj.Error as err:
        print(f'连接数据库失败，联系系统管理员{err}')
        write_log.write_log(f"kingdee_api.get_product_list:连接数据库失败{err}")
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': f'执行语句失败，联系系统管理员{err}'})
    # 获取返回行
    results = connector.fetchall()
    rows_count = len(results)
    print("rows_count=", rows_count)
    if rows_count < 1:
        dbconnect.db_disconnect(connect_obj)
        write_log.write_log("kingdee_api.get_product_list:无数据")
        return json.dumps({"status": 0, "msg": "无数据！"})
    cum_name = ["code", "name", "specification", "unit", "spec_unit", "short_code", "introduction",
                "is_agricultural_product", "net_weight", "product_width", "product_height", "product_length",
                "volume_unit", "volume","one_category_code","one_category_name",
                "category_code","category_name","three_category_code","three_category_name","nc","goods_category_code"]
    result_dic = {'status': 1, 'msg': '查询成功', 'count': rows_count}
    cum_list = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[cum_name[i]] = row[i]
        cum_list.append(temp)
    result_dic['info'] = cum_list
    dbconnect.db_disconnect(connector)
    write_log.write_log(f"kingdee_api.get_product_list:返回{result_dic}")
    return result_dic
