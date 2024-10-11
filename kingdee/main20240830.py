import schedule
import time
import signal
import threading
import send_messageto_kindee
import write_log

# 定义一个全局变量，用于存储上一次任务的调度器
last_job = None


# 程序结束提示信息打印输出
def signal_handler(signum, frame):
    write_log.write_log("明厨亮灶发送数据程序关闭")
    exit(0)


def task_print(prt_str):
    write_log.write_log(prt_str)


# 定义一个运行线程的函数
def run_threaded(job_func, *args):
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()


# 以下是定义线程任务---------------------------------------------------------------------------------------
# 定义定时任务A的执行函数
def task_a(prt_str):
    write_log.write_log('定时任务A开始执行:' + prt_str)
    write_log.write_log('定时任务A执行完成')


# 定义定时线程任每天收货情况
# 将未上传的前100条数据状态置为2
def task_update2_mclz():
    exec_sql = """
    UPDATE  wy_dist_to_mclz
    set state ='2'
    where  bill_number  in 
    (select bill_number from (select  bill_number  from  wy_dist_to_mclz  where state='0' ORDER BY  create_time  limit  100)  s )
        """
    key_name = 'update_mclz'
    send_messageto_kindee.task_update2_mclz(key_name, exec_sql)


#
def task_send_estimated_receivables():
    exec_sql = """
    SELECT	sourceOrg,sourceSys,sourceBillPk,ROUND(SUM(invoiceamt), 2) invoiceamt,org,billtypeid,bizdate,bookdate,completedate,purmode,currency,asstact	,
    recorg,settlementtype,payproperty,remark,department,creator
    from  (
        SELECT  '01' sourceOrg,'01'  sourceSys, b.bill_number  sourceBillPk,
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT) end  as   invoiceamt,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as  org,
        'ar_busbill_standard_BT_S'  billtypeid,DATE_FORMAT(c.dist_date, '%Y-%m-%d')  bizdate,
		CASE  WHEN date_format(c.create_time,'%Y-%m-%d') <= date_format(c.dist_date,'%Y-%m-%d') THEN date_format(c.dist_date,'%Y-%m-%d')
		ELSE date_format(c.create_time,'%Y-%m-%d') END AS bookdate ,
		DATE_FORMAT(b.last_modify_time, '%Y-%m-%d')  completedate,'CREDIT'  purmode,'CNY'  currency,counterpart_unit_code  asstact ,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as recorg ,'JSF299' settlementtype,'1001'payproperty,''remark,''department,'HAIDING-002024'creator
        from  e_account b,e_dist c,e_warehouse_org d
        where  b.bill_number=c.bill_number
		and b.warehouse_code=d.wrh_code 
        and  b.bill_number=%s
		union all
		SELECT  '01' sourceOrg,'01'  sourceSys, b.bill_number  sourceBillPk,
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT) end  as   invoiceamt,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as  org,
        'ar_busbill_standard_BT_S'  billtypeid,
        DATE_FORMAT(c.receive_time, '%Y-%m-%d')  bizdate,DATE_FORMAT(c.receive_time, '%Y-%m-%d')  bookdate,DATE_FORMAT(b.last_modify_time, '%Y-%m-%d')  completedate,
        'CREDIT'  purmode,'CNY'  currency,
        counterpart_unit_code  asstact ,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as recorg ,'JSF299' settlementtype,'1001'payproperty,''remark,''department,'HAIDING-002024'creator
        from  e_account b,e_sale_return c,e_warehouse_org d
        where  b.bill_number=c.bill_number
		and b.warehouse_code=d.wrh_code 
        and  b.bill_number=%s
    )a		
    group by sourceOrg,sourceSys,sourceBillPk,org,billtypeid,bizdate,bookdate,completedate,purmode,currency,asstact,
    recorg,settlementtype,payproperty,remark,department,creator
    HAVING  invoiceamt!=0
    """
    # exec_sql1 = """
    # SELECT CONVERT(tax_rate, FLOAT) taxrate, product_code  e_material,CONVERT(product_price, FLOAT)  price,
    # CONVERT(sum(quantity), FLOAT) quantity
    # from  (
    #     SELECT tax_rate*100  tax_rate,product_price,CONCAT('WYWL-',product_code) product_code,
    #     case when direct='outcome' then  CONVERT(-product_quantity, FLOAT) else CONVERT(product_quantity, FLOAT) end as  quantity
    #     from  e_account
    #     where  bill_number=%s
    # ) a
    # group by tax_rate,e_material,product_price
    # """
    exec_sql1 = """
    SELECT CONVERT(tax_rate, FLOAT) taxrate, product_code  e_material,CONVERT(price, FLOAT)  price,
    CONVERT(sum(quantity), FLOAT) quantity
    from  (
            SELECT tax_rate*100  tax_rate,price,CONCAT('WYWL-',product_code) product_code,receive_quantity  quantity
            from  e_dist_line  
            where  dist_bill_number=%s
    ) a 
    group by tax_rate,e_material,price
    union 
    SELECT CONVERT(tax_rate, FLOAT) taxrate, product_code  e_material,CONVERT(price, FLOAT)  price,
    CONVERT(sum(quantity), FLOAT) quantity
    from  (
            SELECT tax_rate*100  tax_rate,price,CONCAT('WYWL-',product_code) product_code,-receipt_quantity  quantity
            from  e_sale_return_line  
            where  sale_return_bill_number=%s
    ) a 
    group by tax_rate,e_material,price
    """
    url = 'http://180.141.91.139:8023/ierp/kapi/app/ar/ARBusbillWebApiPlugin?access_token='
    key_name = '财务暂估应收单'
    send_messageto_kindee.send_message(key_name, exec_sql, exec_sql1, 1, url)


def task_send_estimated_payables():
    exec_sql = """
    SELECT  sourceOrg,sourceSys,sourceBillPk,billtypeid,isestimated,org,recorg,payproperty,settlementtype,
    department,creator,asstact,currency,asstacttype,paymode,remark,sum(invoiceamt)  invoiceamt
    from (
        SELECT  '01' sourceOrg,'01'  sourceSys, b.bill_number  sourceBillPk,'ap_busbill_stadpur_BT_S'  billtypeid,false isestimated ,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as  org,	
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as recorg ,
        '2001'payproperty,'JSF299' settlementtype,'01050106' department,'HAIDING-002024'creator,counterpart_unit_code  asstact ,'CNY'  currency,
        'bd_supplier' asstacttype,'CREDIT'  paymode,    
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT) end  as   invoiceamt,
        ''remark
        from  e_account b,e_warehouse_org d
        where   b.warehouse_code=d.wrh_code 
        and  b.bill_number=%s
    ) a
    GROUP BY  sourceOrg,sourceSys,sourceBillPk,billtypeid,isestimated,org,recorg,payproperty,settlementtype,
    department,creator,asstact,currency,asstacttype,paymode,remark
    """
    exec_sql1 = """
    SELECT CONVERT(tax_rate, FLOAT) taxrate, product_code  e_material,CONVERT(product_price, FLOAT)  price,
    CONVERT(sum(quantity), FLOAT) quantity
    from  (
    SELECT tax_rate*100  tax_rate,product_price,CONCAT('WYWL-',product_code) product_code,
        case when direct='outcome' then  CONVERT(-product_quantity, FLOAT) else CONVERT(product_quantity, FLOAT) end as  quantity
        from  e_account
        where  bill_number=%s
    ) a 
    group by tax_rate,e_material,product_price
    """
    url = 'http://180.141.91.139:8023/ierp/kapi/app/ap/ApBusbillWebApiService?access_token='
    key_name = '财务暂估应付单'
    send_messageto_kindee.send_message(key_name, exec_sql, exec_sql1, 2, url)


def task_send_financial_payables():
    exec_sql = """ 		
    SELECT  sourceOrg,sourceSys,sourceBillPk,billtypeid,isestimated,qbj8_recumber,org,recorg,payproperty,settlementtype,department,creator,asstact,remark,
	currency,asstacttype,paymode,sum(invoiceamt) invoiceamt
	FROM (
        SELECT  '01' sourceOrg,'01'  sourceSys, b.bill_number  sourceBillPk,'ApFin_pur_BT_S'  billtypeid,true isestimated ,b.rec_bill_number qbj8_recumber,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as  org,	
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as recorg ,
        '2001'payproperty,'JSF299' settlementtype,'01050106' department,'HAIDING-002024'creator,counterpart_unit_code  asstact ,
        ''remark,'CNY'  currency,'bd_supplier' asstacttype,'CREDIT'  paymode, 
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT) end  as   invoiceamt
        from  e_account b,e_warehouse_org d,e_supplier e
        where    b.warehouse_code=d.wrh_code 
        and  b.counterpart_unit_code=e.code
        and  b.bill_number=%s
    ) a
	GROUP BY sourceOrg,sourceSys,sourceBillPk,billtypeid,isestimated,qbj8_recumber,org,recorg,payproperty,settlementtype,department,creator,asstact,remark,
	currency,asstacttype,paymode
    """
    exec_sql1 = """
    SELECT CONVERT(tax_rate, FLOAT) taxrate, product_code  e_material,CONVERT(product_price, FLOAT)  price,
    CONVERT(sum(quantity), FLOAT) quantity
    from  (
    SELECT tax_rate*100  tax_rate,product_price,CONCAT('WYWL-',product_code) product_code,
        case when direct='outcome' then  CONVERT(-product_quantity, FLOAT) else CONVERT(product_quantity, FLOAT) end as  quantity
        from  e_account
        where  bill_number=%s
    ) a 
    group by tax_rate,e_material,product_price
    """
    url = 'http://180.141.91.139:8023/ierp/kapi/app/ap/ApFinapbillWebApiService?access_token='
    key_name = '财务应付单'
    send_messageto_kindee.send_message(key_name, exec_sql, exec_sql1, 3, url)


def task_send_digital_ticketing():
    exec_sql = """		
    select  billNo,billDate,ROUND(SUM(totalAmount), 2) totalAmount,buyerName,buyerCode,buyerTaxpayerId,
	sellerTaxpayerId,sellerName,includeTaxFlag,pushMatchRules,fillValueRule,textField1,textField2
	from (
        SELECT  b.bill_number  billNo,DATE_FORMAT(b.occur_time, '%Y-%m-%d') billDate,
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT)  end  as totalAmount,
        c.jd_customer_name buyerName,c.erpcode buyerCode,c.jd_customer_code  buyerTaxpayerId,
        '91450100MA5NCWQ56A'sellerTaxpayerId,'南宁威耀集采集配供应链管理有限公司'sellerName,
        '1'  includeTaxFlag,'3' pushMatchRules,'goodsCode' fillValueRule,rec_bill_number textField1,c.erpcode  textField2
        from  wy_trans_to_kingdee a, e_account b,e_nc_supplier c
        where   a.bill_number=b.bill_number
		and  b.counterpart_unit_code=c.erpcode
		and  a.bill_number=%s
	) a 
    group by billNo,billDate,buyerName,buyerCode,buyerTaxpayerId,sellerTaxpayerId,sellerName,
	includeTaxFlag,pushMatchRules,fillValueRule,textField1,textField2
    """
    exec_sql1 = """
    SELECT  round(sum(amount),2) amount,sum(quantity) quantity,detailId,goodsCode,lineProperty,price,taxRate,extraField
	from (
        SELECT  CONVERT(price*receive_quantity,FLOAT) amount,a.dist_bill_number detailId,CONCAT('WYWL-',a.product_code)  goodsCode, 
        '2'  lineProperty,CONVERT(a.price, FLOAT)  price,CONVERT(a.receive_quantity, FLOAT)  quantity,CONVERT(a.tax_rate, FLOAT) taxRate,
        case when c.one_code='03' or c.one_code='04' or c.one_code='05' or c.one_code='06' then  IF(RIGHT(one_name, 1) = '类', one_name, CONCAT(one_name, '类'))
        when c.one_code='01' or c.one_code='02' then  IF(RIGHT(two_name, 1) = '类', two_name, CONCAT(two_name, '类'))
        else '其他'  end as extraField
        from  e_dist_line a,e_product b,wy_kingdee_sort c
        where  a.product_code=b.code
        and  b.category_code=c.two_code
        and  a.dist_bill_number=%s
        union all
        SELECT  -CONVERT(price*receipt_quantity,FLOAT) amount,a.sale_return_bill_number detailId,CONCAT('WYWL-',a.product_code)  goodsCode, 
        '2'  lineProperty,CONVERT(a.price, FLOAT)  price,-CONVERT(a.receipt_quantity, FLOAT)  quantity,CONVERT(a.tax_rate, FLOAT) taxRate,
        case when c.one_code='03' or c.one_code='04' or c.one_code='05' or c.one_code='06' then  IF(RIGHT(one_name, 1) = '类', one_name, CONCAT(one_name, '类'))
        when c.one_code='01' or c.one_code='02' then  IF(RIGHT(two_name, 1) = '类', two_name, CONCAT(two_name, '类'))
        else '其他'  end as extraField
        from  e_sale_return_line a,e_product b,wy_kingdee_sort c
        where  a.product_code=b.code
        and  b.category_code=c.two_code
        and  a.sale_return_bill_number=%s
	)  a
	group by  detailId,goodsCode,lineProperty,price,taxRate,extraField
    """
    url = 'http://180.141.91.139:8023/ierp/kapi/app/sim/openApi?access_token='
    key_name = '开票申请单'
    send_messageto_kindee.send_message(key_name, exec_sql, exec_sql1, 4, url)


def task_send_digital_ticketing_return():
    exec_sql = """		
    SELECT  distinct bill_number 
    FROM wy_trans_to_kingdee 
    WHERE bill_type IN ('dist', 'salereturn')
    and statement_state='5'  
    and  bill_number=%s	
    """
    exec_sql1 = """		
    
    """
    url = 'http://180.141.91.139:8023/ierp/kapi/app/sim/openApi?access_token='
    key_name = '开票申请单撤回'
    send_messageto_kindee.send_message(key_name, exec_sql, exec_sql1, 5, url)


# 定义定时线程任  每天凌晨5：00将上传表状态为1（上传成功）的订单备份到wy_dist_to_mclz_log中
# def task_wy_dist_to_mclz():
#     send_mesage.task_wy_dist_to_mclz()


# schedule.every(5).minutes.do(run_threaded, task_print, '定义定时心跳，每5分钟执行一次')
# schedule.every(10).minutes.do(run_threaded, task_update2_mclz)
# schedule.every(30).minutes.do(run_threaded, task_send_mclz)

if __name__ == '__main__':
    write_log.write_log(f'金蝶发送数据程序开始执行')
    # 注册信号处理函数(有其他信号执行signal_handler)
    # signal.signal(signal.SIGINT, signal_handler)
    # 财务暂估应收单
    # task_send_estimated_receivables()
    # 财务暂估应付单
    # task_send_estimated_payables()
    # 财务应付单
    # task_send_financial_payables()
    # 开票申请单
    task_send_digital_ticketing()
    # 开票申请单撤回
    # task_send_digital_ticketing_return()
    # while True:
    #     # 检查是否有定时任务需要执行
    #     schedule.run_pending()
    #     # 其他代码...
    #     time.sleep(1)
