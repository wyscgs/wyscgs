import dbconnect
import write_log
import json
from datetime import datetime


# 获取采购订单详情行
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
        connector.execute(sql_query,(lineuuid,))
    except connect_obj.Error as err:
        dbconnect.db_disconnect(connector)
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
    dbconnect.db_disconnect(connector)
    write_log.write_log(f"supplierapp_api.get_purchase_order_line_details:返回{result_dic}")
    return result_dic


if __name__ == "__main__":
    print(get_purchase_order_line_details('5bf46c3a-6d15-4c54-9e14-845663bbc4a9'))
