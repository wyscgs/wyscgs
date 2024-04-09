# 生成uuid 并写入数据库中，有效期为半小时
import dbconnect
import write_log
import json
import uuid
import datetime


def get_token():
    # 生成一个UUID
    temp_token = uuid.uuid4()
    # 将UUID对象转换为字符串
    token = str(temp_token)
    print(f'token={token}')
    # 获取当前日期和时间
    now = datetime.datetime.now()
    # 计算30分钟后的时间
    new_time = now + datetime.timedelta(minutes=30)
    # 格式化日期和时间
    formatted_date = new_time.strftime("%Y-%m-%d %H:%M:%S")
    # 将格式化后的日期和时间赋值给一个字符串变量
    date_time_string = formatted_date
    # 连接数据库
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("wy_token.get_token:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    sql_query = f"insert into wy_token(uuid,token,create_time) select uuid(),'{token}',sysdate();"
    cur = dbconnect.exec_ddl_sql(connect_obj, sql_query)
    if cur is None:
        dbconnect.db_disconnect(connect_obj)
        return json.dumps({'status': -200, 'meg': 'A获取token失败，联系系统管理员'})
    dbconnect.db_disconnect(connect_obj)
    return json.dumps({'status': 1, 'meg': '获取token成功', 'token': token, 'out_time': date_time_string})


def check_token(token):
    connect_obj = dbconnect.db_connect()
    if connect_obj is None:
        write_log.write_log("wy_token.get_token:连接数据库失败")
        return json.dumps({'status': -100, 'meg': '连接数据库失败，联系系统管理员'})
    sql_query = f"select * from wy_token where token = '{token}' and date_add(create_time, INTERVAL 30 MINUTE) >= sysdate();"
    # cur = dbconnect.exec_sql(connect_obj, sql_query)
    # 创建一个游标
    connector = connect_obj.cursor()
    # sql_query = "select line,batch,CONVERT(quantity, FLOAT) quantity  from  e_purchase_order_line_details  WHERE purchase_line_uuid = %s"
    try:
        connector.execute(sql_query)
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connector)
        print(f'执行SQL语句失败，联系系统管理员{err}')
        return False
    # 获取单据主信息返回行
    results = connector.fetchall()
    rows_count = len(results)
    dbconnect.db_disconnect(connector)
    if rows_count < 1:
        return False
    return True
