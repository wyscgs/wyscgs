import schedule
import time
import signal
import threading
import send_mesage
import write_log


# 程序结束提示信息打印输出
def signal_handler(signum, frame):
    write_log.write_log("企业微信自动发送信息程序关闭")
    exit(0)


# 定义一个运行线程的函数
def run_threaded(job_func, *args):
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()


# 以下是定义县城任务---------------------------------------------------------------------------------------
# 定义定时任务A的执行函数
def task_a(prt_str):
    write_log.write_log('定时任务A开始执行:' + prt_str)
    write_log.write_log('定时任务A执行完成')


def task_print(prt_str):
    write_log.write_log( prt_str)


# 定义定时线程任每天收货情况
# https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=666edf75-db02-401d-b0ef-a48e824678bb
def task_send_wx_allreceipt():
    exec_sql = (
        "select concat(warehouse_name, \'  应收货：\',CONVERT(ROUND(qty,3),CHAR) ,\'吨,已收货：\',convert(ROUND(\nreceived_quantity,3),CHAR),\'吨,未收货：\',"
        "convert(ROUND(unreceived_quantity,3),CHAR),\'吨，收货完成率：\',\nCONVERT(ROUND(rec_percent * 100,2),CHAR),\'%\') as printline \n"
        "from (\nselect a.dist_Date,a.warehouse_code,a.warehouse_name,sum(b.quantity * c.net_weight / 1000000) as qty,\n"
        "sum((b.received_quantity + b.received_gift_quantity)  * c.net_weight/ 1000000) as received_quantity,\n"
        "sum((b.quantity - b.received_quantity  - b.received_gift_quantity) * c.net_weight / 1000000) as unreceived_quantity,\n"
        "case when sum(b.quantity) = 0 then 0 else sum((b.received_quantity + b.received_gift_quantity) * c.net_weight ) / sum(b.quantity * c.net_weight)  end as rec_percent\n"
        "from e_purchase_order a, e_purchase_order_line b,e_product c where a.bill_number = b.purchase_bill_number  \n"
        "and b.product_code = c.code and a.state <> \'aborted\'\n"
        "and DATE_FORMAT(a.dist_date,\'%Y%m%d\') = date_format(DATE_ADD(sysdate(), INTERVAL 22 hour ),\'%Y%m%d\') \n"
        "group by a.dist_Date,a.warehouse_name,a.warehouse_code) a,e_warehouse_org b where a.warehouse_code = b.wrh_code order by b.order_line;")
    key_name = 'allreceipt'
    send_mesage.send_message_to_wechat_robot(key_name, exec_sql)


# 定义定时线程任每天配送情况
# https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=a971354d-2807-4896-b7b8-b69c7dbc9766
def task_send_wx_alldist():
    exec_sql = (
        "select concat(warehouse_name,\' 基地配送学校数：\',CONVERT(ROUND(school_num,0),CHAR) ,\'所，配送量：\',CONVERT(ROUND(ps_d,3),CHAR),\'吨 直送学校：\',\n"
        "CONVERT(ROUND(zsschool_code,0),CHAR) ,\'所，直送量：\',CONVERT(ROUND(zs_d,3),CHAR),\'吨 紧急采购学校：\',\n"
        " CONVERT(ROUND(jjschool_num,0),CHAR) ,\'所，采购量：\',CONVERT(ROUND(jj_d,3),CHAR),\'吨\') as printline\n"
        " from (\n"
        "select b.order_line,a.warehouse_name,sum(school_num) as school_num,sum(ps_d) as ps_d,\n"
        "sum(zsschool_code) as zsschool_code,sum(zs_d) as zs_d,sum(jjschool_num) as jjschool_num,sum(jj_d) as jj_d from \n"
        "(select a.warehouse_code,a.warehouse_name,count(distinct d.hub_school_code) as school_num,sum(b.quantity * c.net_weight) /1000000 as ps_d,\n"
        "0 as zsschool_code,0 as zs_d,0 as jjschool_num,0 as jj_d\n"
        "from e_dist a,e_dist_line b,e_product c,\n"
        "(select code ,case when ifnull(hub_school_code,\'\') = \'\' then code else hub_school_code end  as hub_school_code from e_school) d\n"
        "where a.bill_number = b.dist_bill_number and b.product_code = c.code and a.school_code = d.code\n"
        "and DATE_FORMAT(a.dist_date,\'%y%m%d\')  = DATE_FORMAT(sysdate(),\'%y%m%d\') \n"
        "and dist_source_type = \'WAVE\' group by a.warehouse_code,a.warehouse_name\n"
        "union all \n"
        "select a.warehouse_code,a.warehouse_name,0 as school_num,0 as ps_d,"
        "count(distinct d.hub_school_code)    as zsschool_code,sum(b.quantity * c.net_weight) /1000000  as zs_d,\n"
        "0 as jjschool_num,0 as jj_d\n"
        "from e_dist a,e_dist_line b,e_product c ,\n"
        "(select code ,case when ifnull(hub_school_code,\'\') = \'\' then code else hub_school_code end  as hub_school_code from e_school) d\n"
        "where a.bill_number = b.dist_bill_number and b.product_code = c.code and  a.school_code = d.code\n"
        "and DATE_FORMAT(a.dist_date,\'%y%m%d\')  = DATE_FORMAT(sysdate(),\'%y%m%d\') \n"
        "and dist_source_type = \'DIRECT_PURCHASE_ORDER\' group by a.warehouse_code,a.warehouse_name\n"
        "union all \n"
        "select a.warehouse_code,a.warehouse_name,0 as school_num,0 as ps_d,0  as zsschool_code,0  as zs_d,\n"
        "count(distinct d.hub_school_code)  as jjschool_num,sum(b.quantity * c.net_weight) /1000000 as jj_d\n"
        "from e_dist a,e_dist_line b,e_product c ,\n"
        "(select code ,case when ifnull(hub_school_code,\'\') = \'\' then code else hub_school_code end  as hub_school_code from e_school) d\n"
        "where a.bill_number = b.dist_bill_number and b.product_code = c.code and  a.school_code = d.code\n"
        "and DATE_FORMAT(a.dist_date,\'%y%m%d\')  = DATE_FORMAT(sysdate(),\'%y%m%d\') \n"
        "and dist_source_type = \'EMERGENT_PURCHASE_ORDER\' group by a.warehouse_code,a.warehouse_name) a ,e_warehouse_org b \n"
        "where a.warehouse_code = b.wrh_code  group by a.warehouse_name,b.order_line\n"
        "order by b.order_line )a\n ")
    key_name = 'alldist'
    send_mesage.send_message_to_wechat_robot(key_name, exec_sql)




def task_send_product_info():
    exec_sql = """
    select   '总计',COUNT(*)  number
    from  e_product
    where state='USING'
    UNION
    select product_event,count(product_code) number from(
    select c.code as product_code,c.name as product_name,ifnull(d.owner_uuid,'无上传商品图片') as product_event
    from e_purchase_catalog a,e_purchase_catalog_line b,e_product c left outer join (select owner_type,owner_uuid from e_accessory  where owner_type = 'Product' group by owner_type,owner_uuid) d on c.uuid = d.owner_uuid
    where a.uuid=b.purchase_catalog_uuid and b.product_code=c.code and a.begin_date<=curdate() and a.end_date>=curdate() and d.owner_uuid is null
    union
    select c.code as product_code,c.name as product_name,ifnull(c.shelf_life_value,'无保质期') as product_event
    from e_purchase_catalog a,e_purchase_catalog_line b,e_product c 
    where a.uuid=b.purchase_catalog_uuid and b.product_code=c.code and a.begin_date<=curdate() and a.end_date>=curdate() and c.shelf_life_value is null
    )q GROUP BY  product_event
    """
    key_name = 'product_info'
    send_mesage.send_message_to_wechat_robot_product_info(key_name, exec_sql)


def task_send_trial_supplier_info():
    exec_sql = """
    SELECT  concat("供应商编号：",code,"，供应商名称：",name,",",remark,"，距离试供结束还剩",DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) ,"天。")  printline
    from  e_supplier 
    where  remark like '%试供%'
    AND DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) <= 20
    AND DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) >= 0
    union all
    SELECT  concat("供应商编号：",code,"，供应商名称：",name,",",remark,"，已结束",DATEDIFF(CURDATE(),STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s')) ,"天。")  printline
    from  e_supplier 
    where  remark like '%试供%'
    AND DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) < 0
    """
    key_name = 'trial_supplier_info'
    send_mesage.send_message_to_wechat_robot_trial_supplier_info(key_name, exec_sql)



def task_send_trial_supplier_info():
    exec_sql = """
    SELECT  concat("供应商编号：",code,"，供应商名称：",name,",",remark,"，距离试供结束还剩",DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) ,"天。")  printline
    from  e_supplier 
    where  remark like '%试供%'
    AND DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) <= 20
    AND DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) >= 0
    union all
    SELECT  concat("供应商编号：",code,"，供应商名称：",name,",",remark,"，已结束",DATEDIFF(CURDATE(),STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s')) ,"天。")  printline
    from  e_supplier 
    where  remark like '%试供%'
    AND DATEDIFF(STR_TO_DATE(SUBSTRING(remark, 3, 10), '%Y-%m-%d %H:%i:%s'),CURDATE()) < 0
    """
    key_name = 'trial_supplier_info'
    send_mesage.send_message_to_wechat_robot_trial_supplier_info(key_name, exec_sql)


def task_send_purchase_catalog():
    exec_sql = """
    SELECT CONCAT("配送日期：",dist_date,"仓库编码：",warehouse_code,"仓库名称：",warehouse_name,"商品编码：",product_code,"商品名称：",product_name,"无销售目录") printline
    from (
    select distinct sa.dist_date ,sa.warehouse_code ,sa.warehouse_name ,sal.product_code ,sal.product_name
    from e_sale_order sa,e_sale_order_line sal,e_product ep
    where sal.order_bill_number=sa.bill_number
    and sa.type in ('breakfastDinner')
    and sa.state<>'deleted'
    and sal.product_code=ep.code
    and ep.is_finish_product=0
    and sa.dist_date>=curdate()+2
    and sa.dist_date<=curdate()+3
    and not exists(select 1 from e_purchase_catalog pc,e_purchase_catalog_line pcl,e_sale_order sa
    where pcl.purchase_catalog_uuid = pc.uuid and pcl.warehouse_code = sa.warehouse_code
    and sal.product_code = pcl.product_code and pcl.enable=1
    and pc.begin_date <= sa.dist_date
    and pc.end_date >= sa.dist_date
    )  
	)mm
    ORDER BY dist_date ,warehouse_code
    """
    key_name = 'purchase_catalog_info'
    send_mesage.send_message_to_wechat_robot_purchase_catalog(key_name, exec_sql)

# 定义线程任务结束--------------------------------------------------------------------------------------------------------


# 创建定时任务——————————————————————————————————————————————————————————————————————————————————————————————————————————————————
# 创建并运行定时任务A
# schedule.every(5).seconds.do(run_threaded, task_a, "每5秒执行一次有参数的线程")


# 每天中午12点至凌晨2点定时发送收货数据到群中
schedule.every().day.at("18:00").do(run_threaded, task_send_wx_allreceipt)
schedule.every().day.at("19:00").do(run_threaded, task_send_wx_allreceipt)
schedule.every().day.at("20:00").do(run_threaded, task_send_wx_allreceipt)
schedule.every().day.at("21:00").do(run_threaded, task_send_wx_allreceipt)
schedule.every().day.at("22:00").do(run_threaded, task_send_wx_allreceipt)
schedule.every().day.at("23:00").do(run_threaded, task_send_wx_allreceipt)
schedule.every().day.at("00:00").do(run_threaded, task_send_wx_allreceipt)
schedule.every().day.at("01:00").do(run_threaded, task_send_wx_allreceipt)

# 每天早上8点发送每日配送量数据到群中
# schedule.every().day.at("12:00").do(run_threaded,task_send_wx_alldist)

#每周一早上8：00推送给商品信息至采购群里
schedule.every().monday.at("08:00").do(run_threaded, task_send_product_info)

#每天早上10：00推送给试供供应商信息至采购群里
schedule.every().day.at("10:00").do(run_threaded,task_send_trial_supplier_info)

#每天10：00/16:00推送给是否存在缺失采购目录的商品至采购群里 BY LB
schedule.every().day.at("08:00").do(run_threaded,task_send_purchase_catalog)
schedule.every().day.at("16:10").do(run_threaded,task_send_purchase_catalog)
# schedule.every().day.at("10:00").do(run_threaded,task_send_purchase_catalog)
# 定义定时任务，每隔5秒执行一次
# schedule.every(5).seconds.do(print_hi_sleep,f'定义定时任务，每隔5秒执行一次A')
# 定义定时任务，每分钟的第10秒执行一次
# schedule.every().minute.at(":10").do(print_hi,'定义定时任务，每分钟的第10秒执行一次')
# 定义定时任务，每小时的第30分钟执行一次
schedule.every().hour.at(":00").do(run_threaded,task_print,'定义定时心跳，每小时执行一次')
# 定义定时任务，每天的10:30执行一次
# schedule.every().day.at("10:30").do(print_hi,'定义定时任务，每天的10:30执行一次')
# 定义定时任务，每周一的10:30执行一次
# schedule.every().monday.at("10:30").do(print_hi,'定义定时任务，每周一的10:30执行一次')
# 定义定时任务，每月的1号的10:30执行一次
# schedule.every().month.at("10:30").do(print_hi,'定义定时任务，每月的1号的10:30执行一次')
# ——————————————————————————————————————————————————————————————————————————————————————————————————————————


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    write_log.write_log(f'企业微信自动发送信息程序开始执行')
    # 注册信号处理函数
    signal.signal(signal.SIGINT, signal_handler)
    while True:
        # 检查是否有定时任务需要执行
        schedule.run_pending()
        # 其他代码...
        time.sleep(1)
