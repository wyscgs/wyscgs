import schedule
import time
import signal
import threading
import send_mesage
import write_log
from datetime import datetime, timedelta


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



# 企业微信采购目录中商品缺商品图片/重量/保质期定时推送
def task_send_product_info():
    exec_sql = """
    select   '总计',COUNT(*)  number
    from  e_product
    where state='USING'
    UNION
    select product_event,count(product_code) number from(
    select c.code as product_code,c.name as product_name,'无上传商品图片' as product_event
    from e_purchase_catalog a,e_purchase_catalog_line b,e_product c left outer join (select owner_type,owner_uuid from e_accessory  where owner_type = 'Product' group by owner_type,owner_uuid) d on c.uuid = d.owner_uuid
    where a.uuid=b.purchase_catalog_uuid and b.product_code=c.code and a.begin_date<=curdate() and a.end_date>=curdate() and d.owner_uuid is null
    union
    select c.code as product_code,c.name as product_name,'无保质期' as product_event
    from e_purchase_catalog a,e_purchase_catalog_line b,e_product c 
    where a.uuid=b.purchase_catalog_uuid and b.product_code=c.code and a.begin_date<=curdate() and a.end_date>=curdate() and c.shelf_life_value is null
	union
    select c.code as product_code,c.name as product_name,'无重量' as product_event
    from e_purchase_catalog a,e_purchase_catalog_line b,e_product c 
    where a.uuid=b.purchase_catalog_uuid and b.product_code=c.code and a.begin_date<=curdate() and a.end_date>=curdate() 
	and (c.net_weight=0 or c.net_weight is null)
	union
    select c.code as product_code,c.name as product_name,'无说明' as product_event
    from e_purchase_catalog a,e_purchase_catalog_line b,e_product c 
    where a.uuid=b.purchase_catalog_uuid and b.product_code=c.code and a.begin_date<=curdate() and a.end_date>=curdate() 
	and (c.introduction='' or c.introduction is null)
    )q GROUP BY  product_event
    """
    key_name = 'product_info'
    send_mesage.send_message_to_wechat_robot_product_info(key_name, exec_sql)

# 试供供应商提前20天结束的信息推送
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

# 缺采购目录的商品推送
def task_send_purchase_catalog():
    exec_sql = """
    select distinct DATE_FORMAT(a.dist_date, '%Y-%m-%d')  as dist_date,a.warehouse_code,a.warehouse_name,b.product_code,b.product_name
    from e_sale_order a,e_sale_order_line b,e_product c
    where b.order_bill_number=a.bill_number
    and a.type='breakfastDinner'
    and a.state<>'deleted'
    and b.product_code=c.code
    and c.is_finish_product=0
    and a.dist_date>=DATE_ADD(CURDATE(), INTERVAL 2 DAY)
    and a.dist_date<=DATE_ADD(CURDATE(), INTERVAL 3 DAY)
    and not exists(select 1 from e_purchase_catalog d join e_purchase_catalog_line e
    on e.purchase_catalog_uuid = d.uuid
    where e.warehouse_code = a.warehouse_code and b.product_code = e.product_code
    and e.enable=1
    and d.begin_date <= a.dist_date
    and d.end_date >= a.dist_date
    )
    """
    key_name = 'purchase_catalog_info'
    send_mesage.send_message_to_wechat_robot_purchase_catalog(key_name, exec_sql)


# 基地波次状态推送
def task_send_wave_state_info():
    # 获取当前日期时间,界定每天22点时之后为为后一天日期
    current_datetime = datetime.now()
    # 加上2小时
    new_datetime = current_datetime + timedelta(hours=2)
    # 将日期时间转换成字符串
    new_datetime_str = new_datetime.strftime('%Y-%m-%d')

    exec_sql = """
    select hz.wrh_code,hz.wrh_name,hz.bill_number,hz.state,hz.pickup_count,hz.unlocks,hz.unpicked,hz.picked,hz.checked,hz.suspend,hz.canceled
    from (select a.uuid,a.dist_date,a.warehouse_code wrh_code,a.warehouse_name wrh_name,a.bill_number,case a.sale_order_type when 'unpack' then '零星波次' else '食材波次' end as sale_order_type,
    a.remark,a.create_time,a.create_oper_code,a.create_oper_name,a.dist_count,a.pickup_count,a.sale_order_count,
    case a.state when 'initial' then '初始化' when 'finished' then '已完成' end as state,
    sum(case when b.state ='picked' then 1 else 0 end ) as picked,
    sum(case when b.state ='deleted' then 1 else 0 end ) as deleted,
    sum(case when b.state ='canceled' then 1 else 0 end ) as canceled,
    sum(case when b.state ='checked' then 1 else 0 end ) as checked,
    sum(case when b.state ='unpicked' then 1 else 0 end ) as unpicked,
    sum(case when b.state ='suspend' then 1 else 0 end ) as suspend,
    sum(case when b.state ='unlock' then 1 else 0 end ) as unlocks
    from e_waves a , e_pick_pool b
    where a.bill_number = b.waves_bill_number
    and a.dist_date = '{}'
    group by a.dist_date,a.warehouse_code,a.warehouse_name,a.bill_number,a.sale_order_type,a.remark,a.create_time,a.create_oper_code,a.create_oper_name,a.dist_count,a.pickup_count,a.sale_order_count,a.state
    )hz left join e_operate_log c on hz.uuid=c.owner_uuid and c.o_describe='结束波次成功'
    where 1=1
    and  hz.sale_order_count>=10
    and  state='初始化'
    and  hz.wrh_code<>'W0010'
    order by hz.wrh_name,hz.wrh_code,hz.bill_number
    """.format(new_datetime_str)
    key_name = 'wave_state_info'
    send_mesage.send_message_to_wechat_robot_send_wave_state_info(key_name, exec_sql)

def task_send_dist_weight():
    #wgy 每日学校配送重量定时传企业微信

    exec_sql = """
    select 
    /*按当天*/
    DATE_FORMAT(curdate(),'%Y-%m-%d') as expiration_date,
    round(sum(q.day_dist_amount)/10000,2) as day_dist_amount,/*单位为万元*/
    round(sum(q.day_dist_weight),2) as day_dist_weight,
    /*按周*/
    concat(DATE_FORMAT(curdate()-6,'%Y-%m-%d'),'至',DATE_FORMAT(curdate(),'%Y-%m-%d')) as week_date,
    round(sum(q.week_dist_amount)/10000,2) as week_dist_amount,
    round(sum(q.week_dist_weight),2) as week_dist_weight,
    /*按月份*/
    DATE_FORMAT(curdate(),'%Y-%m') as month_date,
    round(sum(q.month_dist_amount)/10000,2) as month_dist_amount,
    round(sum(q.month_dist_weight),2) as month_dist_weight,
    /*按学期*/
    concat('2024-09-01','至',DATE_FORMAT(curdate(),'%Y-%m-%d')) as semester_date,
    round(sum(q.semester_dist_amount)/10000,2) as semester_dist_amount,
    round(sum(q.semester_dist_weight),2) as semester_dist_weight,
	/*按2024年*/
    concat('2024-01-01','至',DATE_FORMAT(curdate(),'%Y-%m-%d')) as year_date,
    round(sum(q.year_dist_amount)/10000,2) as year_dist_amount,
    round(sum(q.year_dist_weight),2) as year_dist_weight,
    /*从开业至2024年8月1日前累计重量为26万吨*/
    round(sum(q.all_dist_weight)+260000,2) as all_dist_weight
    from (
    /*按天累计*/
    select sum(dist_amount) as day_dist_amount,sum(dist_weight)/1000 as day_dist_weight,'' as week_dist_amount,'' as week_dist_weight,'' as month_dist_amount,'' as month_dist_weight,'' as semester_dist_amount,'' as semester_dist_weight,'' as year_dist_amount,'' as year_dist_weight,'' as all_dist_weight
    from wy_dist_weight where dist_date=curdate()
    /*按周累计*/
    union all
    select '' as day_dist_amount,'' as day_dist_weight,sum(dist_amount) as week_dist_amount,sum(dist_weight)/1000 as week_dist_weight,'' as month_dist_amount,'' as month_dist_weight,'' as semester_dist_amount,'' as semester_dist_weight,'' as year_dist_amount,'' as year_dist_weight,'' as all_dist_weight
    from wy_dist_weight 
    where dist_date>=curdate()-6 and dist_date<=curdate()
    /*按月累计*/
    union all
    select '' as day_dist_amount,'' as day_dist_weight,'' as week_dist_amount,'' as week_dist_weight,sum(dist_amount) as month_dist_amount,sum(dist_weight)/1000 as month_dist_weight,'' as semester_dist_amount,'' as semester_dist_weight,'' as year_dist_amount,'' as year_dist_weight,'' as all_dist_weight
    from wy_dist_weight 
    where DATE_FORMAT(dist_date,'%Y-%m')=DATE_FORMAT(curdate(),'%Y-%m')
    /*按学期累计*/
    union all
    select '' as day_dist_amount,'' as day_dist_weight,'' as week_dist_amount,'' as week_dist_weight,'' as month_dist_amount,'' as month_dist_weight,sum(dist_amount) as semester_dist_amount,sum(dist_weight)/1000 as semester_dist_weight,'' as year_dist_amount,'' as year_dist_weight,'' as all_dist_weight
    from wy_dist_weight 
    where dist_date>='2024-09-01'
    /*按年累计*/
    union all
    select '' as day_dist_amount,'' as day_dist_weight,'' as week_dist_amount,'' as week_dist_weight,'' as month_dist_amount,'' as month_dist_weight,'' as semester_dist_amount,'' as semester_dist_weight,sum(dist_amount) as year_dist_amount,sum(dist_weight)/1000 as year_dist_weight,'' as all_dist_weight
    from wy_dist_weight 
    where dist_date>='2024-01-01'		
	/*全部累计*/
    union all
    select '' as day_dist_amount,'' as day_dist_weight,'' as week_dist_amount,'' as week_dist_weight,'' as month_dist_amount,'' as month_dist_weight,'' as semester_dist_amount,'' as semester_dist_weight,'' as year_dist_amount,'' as year_dist_weight,sum(dist_weight)/1000 as all_dist_weight
    from wy_dist_weight 
    where dist_date>='2024-08-01'
    )q
    """
    key_name = 'send_dist_weight'
    send_mesage.send_message_to_wechat_robot_send_dist_weight(key_name, exec_sql)

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

#每天10：00/16:00/16:50推送给是否存在缺失采购目录的商品至采购群里 BY LB
schedule.every().day.at("08:00").do(run_threaded,task_send_purchase_catalog)
schedule.every().day.at("16:00").do(run_threaded,task_send_purchase_catalog)
schedule.every().day.at("16:50").do(run_threaded,task_send_purchase_catalog)
schedule.every().day.at("16:59").do(run_threaded,task_send_purchase_catalog)


#每天23：00/00:00/1:00/2：00  推送给是基地波次状态到信息群 BY LB
# schedule.every().day.at("11:27").do(run_threaded,task_send_wave_state_info)
# schedule.every().day.at("23:30").do(run_threaded,task_send_wave_state_info)
# schedule.every().day.at("00:00").do(run_threaded,task_send_wave_state_info)
# schedule.every().day.at("00:30").do(run_threaded,task_send_wave_state_info)
schedule.every().day.at("01:00").do(run_threaded,task_send_wave_state_info)
# schedule.every().day.at("01:30").do(run_threaded,task_send_wave_state_info)
schedule.every().day.at("02:00").do(run_threaded,task_send_wave_state_info)
# schedule.every().day.at("02:30").do(run_threaded,task_send_wave_state_info)

#每天16：10 推送每日配送重量到信息群 WGY
schedule.every().day.at("16:10").do(run_threaded,task_send_dist_weight)


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
