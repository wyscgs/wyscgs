from server_conf import *
import json


def get_purchase_order(code):
    query_sql = """
                select bill_number,DATE_FORMAT(dist_date, '%Y-%m-%d') dist_date,supplier_name,warehouse_code,warehouse_name,
                case when state = 'finished' then '已完成' when state = 'partialReceived' then '部分收货' when state = 'checked' then '已审核' else '其他' end  as state,is_system_auto_push,
                CONVERT(sum(plan_quantity) , char)  plan_quantity, CONVERT(sum(received_quantity) , char)  received_quantity from (
                SELECT  a.dist_date,a.bill_number,a.supplier_name,a.warehouse_code,a.warehouse_name,a.state,case when is_system_auto_push =1 then '是' else '否' end as is_system_auto_push,total_count,b.plan_quantity,b.purchase_bill_number,b.quantity,b.received_quantity
                from e_purchase_order a,e_purchase_order_line b 
                where  a.bill_number=b.purchase_bill_number
                and  a.state in('checked','partialReceived','finished')
                and is_direct_delivery=0
                and  a.bill_number='{}'
                ) ss
                GROUP BY bill_number,dist_date,supplier_name,warehouse_code,warehouse_name,state,is_system_auto_push
                order  by  dist_date  desc
                """.format(code)
    conn = create_mysql_conn()
    # 创建一个游标
    cursor = conn.cursor()
    n = cursor.execute(query_sql)
    if n == 0:
        cursor.close()
        conn.close()
        return json.dumps({"status": "请输入正确采购单号！"})
    else:
        ls = ['bill_number', 'dist_date', 'supplier_name', 'warehouse_code', 'warehouse_name', 'state',
              'is_system_auto_push', 'plan_quantity', 'received_quantity']
        result_dic = {'count': n, 'code': code}
        for row in cursor:
            for i in range(len(row)):
                result_dic[ls[i]] = row[i]
        cursor.close()
        conn.close()
    return result_dic


# {
#     "count": 1,
#     "code": "CD202410040001",
#     "bill_number": "CD202410040001",
#     "dist_date": "2022-10-05",
#     "supplier_name": "金喜地",
#     "warehouse_code": "W0010",
#     "warehouse_name": "南宁干杂仓",
#     "state": "部分收货",
#     "is_system_auto_push": "否",
#     "plan_quantity": "3040.000",
#     "received_quantity": "2440.000"
# }

def get_receipt_order(code):
    query_sql = """
                select  bill_number,state,receipt_number,create_oper_name,DATE_FORMAT(receipt_time, '%Y-%m-%d')  receipt_time,supplier_name,CONVERT(sum(quantity),char)  quantity  from (
                select  c.bill_number,case when c.state = 'finished' then '已完成' when c.state = 'partialReceived' then '部分收货'  else '其他' end  as state,
                a.bill_number  receipt_number,a.create_oper_name, a.receipt_time,c.supplier_name,b.quantity
                from  e_receipt a,e_receipt_line b,e_purchase_order  c
                where  a.bill_number=b.receipt_bill_number
                and a.purchase_bill_number=c.bill_number
                and a.bill_number='{}'
                )ss 
                group by bill_number,state,receipt_number,create_oper_name,receipt_time,supplier_name
                """.format(code)
    conn = create_mysql_conn()
    # 创建一个游标
    cursor = conn.cursor()
    n = cursor.execute(query_sql)
    if n == 0:
        cursor.close()
        conn.close()
        return json.dumps({"status": "请输入正确采购单号！"})
    else:
        ls = ['bill_number', 'state', 'receipt_number', 'create_oper_name', 'receipt_time', 'supplier_name',
              'quantity']
        result_dic = {'count': n, 'code': code}
        ls_2 = []
        for row in cursor:
            temp = {}
            for i in range(len(row)):
                temp[ls[i]] = row[i]
            ls_2.append(temp)
        result_dic['info'] = ls_2
        cursor.close()
        conn.close()
    return result_dic
