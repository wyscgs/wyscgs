import hashlib
import dbconnect as dbcon
from datetime import datetime, timedelta


def chagedatetime(time):
    timestamp = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ')
    timestamp += timedelta(hours=8)
    mysql_datetime_format = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_datetime_format


def checknone(variable):
    if variable is None:
        null_variable = 0.0
    else:
        null_variable = variable
    return null_variable


def get_signature(nonce, payload, secret, timestamp):
    content = ':'.join([nonce, payload, secret, timestamp]).encode('utf-8')
    m = hashlib.sha1()
    m.update(content)
    return m.hexdigest()


def handle(payload):
    if payload['op'] == 'data_create':
        return add(payload['data'])
    if payload['op'] == 'data_update':
        return update(payload['data'])
    if payload['op'] == 'data_remove':
        return remove(payload['data'])


def add(data):
    entryId = data['entryId']
    sql = ""
    if entryId == '6584f574ef3f644493ed2884':  # 供应商投单表
        data_id = data['_id']
        td_number = data['td_number']
        purchase_bill_number = data['purchase_bill_number']
        supplier_name = data['supplier_name']
        weight = data['weight']
        warehouse_name = data['warehouse_name']
        plan_strat_time = data['plan_strat_time']
        plan_end_time = data['plan_end_time']
        dist_date = chagedatetime(data['dist_date'])
        toudan_time = chagedatetime(data['toudan_time'])
        attendance_status = data['attendance_status']
        late_time = checknone(data['late_time'])
        early_time = checknone(data['early_time'])
        remark = data['remark']
        # sign_confirm = data['sign_confirm']
        create_time = chagedatetime(data['createTime'])
        order_state = data['order_state']
        category_name = data['category_name']
        sql = """insert into `wy_lgd_td` (data_id,td_number,purchase_bill_number,supplier_name,weight,warehouse_name,plan_strat_time,plan_end_time,dist_date,
        toudan_time,attendance_status,late_time,early_time,remark,create_time,
        order_state,category_name)
        values ("{}", "{}", "{}", "{}","{}", "{}", "{}", "{}","{}", "{}", "{}","{}","{}", "{}", "{}", "{}", "{}")
         """.format(data_id, td_number, purchase_bill_number, supplier_name, weight, warehouse_name, plan_strat_time,
                    plan_end_time, dist_date,
                    toudan_time, attendance_status, late_time, early_time, remark, create_time,
                    order_state, category_name)
    if entryId == '6536391c0d7d889bd4a3f914':  # 收货员接单表
        data_id = data['_id']
        td_number = data['td_number']
        purchase_bill_number = data['purchase_bill_number']
        toudan_time = chagedatetime(data['toudan_time'])
        jd_time = chagedatetime(data['jd_time'])
        pd_time = data['pd_time']
        jd_remark = data['jd_remark']
        create_time = chagedatetime(data['createTime'])

        sql = """insert into `wy_lgd_jd` (data_id,td_number,purchase_bill_number,toudan_time,jd_time,pd_time,jd_remark,
        create_time)
        values ("{}", "{}", "{}", "{}","{}", "{}", "{}", "{}")
        """.format(data_id, td_number, purchase_bill_number, toudan_time, jd_time, pd_time, jd_remark, create_time)
    conn = dbcon.db_connect()
    try:
        print(sql)
        cursor = dbcon.exec_sql(conn, sql)
        conn.commit()
        print("成功")
        cursor.close()
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()


def update(data):
    entryId = data['entryId']
    oid = data['_id']
    sql = ""
    if entryId == '6584f574ef3f644493ed2884':  # 供应商投单表
        plan_strat_time = data['plan_strat_time']
        plan_end_time = data['plan_end_time']
        toudan_time = chagedatetime(data['toudan_time'])
        attendance_status = data['attendance_status']
        late_time = checknone(data['late_time'])
        early_time = checknone(data['early_time'])
        order_state = data['order_state']
        # # category_name = data['category_name']
        sql = 'update `wy_lgd_td` set plan_strat_time = "%s", plan_end_time = "%s", toudan_time = "%s", attendance_status = "%s", late_time = "%s", early_time = "%s", order_state = "%s" where data_id = "%s"' % \
              (plan_strat_time, plan_end_time, toudan_time, attendance_status, late_time, early_time, order_state, oid)
    if entryId == '6536391c0d7d889bd4a3f914':  # 收货员接单表
        toudan_time = chagedatetime(data['toudan_time'])
        jd_time = chagedatetime(data['jd_time'])
        pd_time = data['pd_time']
        sql = 'update `wy_lgd_jd` set toudan_time = "%s", jd_time = "%s", pd_time = "%s" where data_id = "%s"' % \
              (toudan_time, jd_time, pd_time, oid)
    conn = dbcon.db_connect()
    try:
        print(sql)
        cursor = dbcon.exec_sql(conn, sql)
        conn.commit()
        print("成功")
        cursor.close()
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()


def remove(data):
    oid = data['_id']
    entryId = data['entryId']
    sql = ""
    if entryId == '6584f574ef3f644493ed2884':  # 供应商投单表
        sql = 'delete from `wy_lgd_td` where data_id="%s"' % (oid)
    if entryId == '6536391c0d7d889bd4a3f914':  # 收货员接单表
        sql = 'delete from `wy_lgd_jd` where data_id="%s"' % (oid)
    conn = dbcon.db_connect()
    try:
        print(sql)
        cursor = dbcon.exec_sql(conn, sql)
        conn.commit()
        print("成功")
        cursor.close()
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()
