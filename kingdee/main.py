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
    SELECT	sourceOrg,sourceSys,sourceBillPk,sum(invoiceamt) invoiceamt,org,billtypeid,bizdate,purmode,currency,asstact	,
    recorg,settlementtype,payproperty,remark,department,creator
    from  (
        SELECT  '01' sourceOrg,'01'  sourceSys, b.bill_number  sourceBillPk,
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT) end  as   invoiceamt,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as  org,
        'ar_busbill_standard_BT_S'  billtypeid,DATE_FORMAT(b.occur_time, '%Y-%m-%d')  bizdate,'CASH'  purmode,'CNY'  currency,
        '91450200794328218E'  asstact ,
        case when d.fcode='F001' then '010H0J'   when d.fcode='F002' then '010H0J0A'   when d.fcode='F003' then '010H0J0B'
        when d.fcode='F004' then '010H0J0F'   when d.fcode='F005' then '010H0J0G'   when d.fcode='F006' then '010H0J0L'
        when d.fcode='F007' then '010H0J0M'  else '010H0J' end  as recorg ,'JSF299' settlementtype,'1001'payproperty,'测试测试'remark,''department,'ID-002038'creator
        from  e_account b,e_warehouse_org d
        where  b.warehouse_code=d.wrh_code 
        and  b.bill_number=%s
    )a		
    group by sourceOrg,sourceSys,sourceBillPk,org,billtypeid,bizdate,purmode,currency,asstact	,
    recorg,settlementtype,payproperty,remark,department,creator
    """
    exec_sql1 = """
    SELECT CONVERT(tax_rate, FLOAT) taxrate, '101010101'  e_material,CONVERT(product_price, FLOAT)  price,
    CONVERT(sum(quantity), FLOAT) quantity
    from  (
        SELECT tax_rate*100  tax_rate,product_price,
        case when direct='outcome' then  CONVERT(-product_quantity, FLOAT) else CONVERT(product_quantity, FLOAT) end as  quantity
        from  e_account
        where  bill_number=%s
    ) a 
    group by tax_rate,e_material,product_price
    """
    url = 'http://180.141.91.139:8022/ierp/kapi/app/ar/ArBusbillWebApiService?access_token='
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
        '2001'payproperty,'JSF299' settlementtype,'01050106' department,'ID-002038'creator,'91450103763071958G'  asstact ,'CNY'  currency,
        'bd_supplier' asstacttype,'CASH'  paymode,    
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT) end  as   invoiceamt,
        '测试测试'remark
        from  e_account b,e_warehouse_org d
        where   b.warehouse_code=d.wrh_code 
        and  b.bill_number=%s
    ) a
    GROUP BY  sourceOrg,sourceSys,sourceBillPk,billtypeid,isestimated,org,recorg,payproperty,settlementtype,
    department,creator,asstact,currency,asstacttype,paymode,remark
    """
    exec_sql1 = """
    SELECT CONVERT(tax_rate, FLOAT) taxrate, '101010101'  e_material,CONVERT(product_price, FLOAT)  price,
    CONVERT(sum(quantity), FLOAT) quantity
    from  (
    SELECT tax_rate*100  tax_rate,product_price,
        case when direct='outcome' then  CONVERT(-product_quantity, FLOAT) else CONVERT(product_quantity, FLOAT) end as  quantity
        from  e_account
        where  bill_number=%s
    ) a 
    group by tax_rate,e_material,product_price
    """
    url = 'http://180.141.91.139:8022/ierp/kapi/app/ap/ApBusbillWebApiService?access_token='
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
        '2001'payproperty,'JSF299' settlementtype,'01050106' department,'ID-002038'creator,'91450103763071958G'  asstact ,
        '测试测试'remark,'CNY'  currency,'bd_supplier' asstacttype,'CASH'  paymode, 
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
    SELECT CONVERT(tax_rate, FLOAT) taxrate, '101010101'  e_material,CONVERT(product_price, FLOAT)  price,
    CONVERT(sum(quantity), FLOAT) quantity
    from  (
    SELECT tax_rate*100  tax_rate,product_price,
        case when direct='outcome' then  CONVERT(-product_quantity, FLOAT) else CONVERT(product_quantity, FLOAT) end as  quantity
        from  e_account
        where  bill_number=%s
    ) a 
    group by tax_rate,e_material,product_price
    """
    url = 'http://180.141.91.139:8022/ierp/kapi/app/ap/ApFinapbillWebApiService?access_token='
    key_name = '财务应付单'
    send_messageto_kindee.send_message(key_name, exec_sql, exec_sql1, 3, url)


def task_send_digital_ticketing():
    # exec_sql = """
    # SELECT  a.bill_number  billNo,DATE_FORMAT(b.occur_time, '%Y-%m-%d') billDate,
    # case when a.bill_type='salereturn' then CONVERT(-sum(b.agg_total), FLOAT)   else CONVERT(sum(b.agg_total), FLOAT) end  as totalAmount,
    # '0' autoInvoice,'028' invoiceType,b.counterpart_unit_name buyerName,b.counterpart_unit_code buyerCode,c.taxpayer_identification_number buyerTaxpayerId ,'0'  buyerProperty,
    # case when d.fname='南宁威耀集采集配供应链管理有限公司（南宁）' then '南宁威耀集采集配供应链管理有限公司（本部）' else d.fname end as sellerName,
    # '1'  includeTaxFlag,'0' pushMatchRules,rec_bill_number textField1
    # from  wy_trans_to_kingdee  a,e_account b,e_school_invoice_info c,e_warehouse_org d
    # where  a.bill_number=b.bill_number
    # and b.counterpart_unit_code=c.school_code
    # and  b.warehouse_code=d.wrh_code
    # and  a.bill_type  in  ('dist','salereturn')
    # and  a.bill_number=%s
    # group by a.bill_number ,b.occur_time,a.bill_type, autoInvoice,invoiceType,b.counterpart_unit_name,
    # b.counterpart_unit_code,c.taxpayer_identification_number , buyerProperty,d.fname,includeTaxFlag,pushMatchRules,rec_bill_number
    # """
    exec_sql = """ 		
	select  billNo,billDate,CONVERT(sum(totalAmount),FLOAT) totalAmount,autoInvoice,invoiceType,buyerName,buyerCode,buyerTaxpayerId,buyerProperty,
	sellerTaxpayerId,sellerName,includeTaxFlag,pushMatchRules,textField1
	from (
        SELECT  a.bill_number  billNo,DATE_FORMAT(b.occur_time, '%Y-%m-%d') billDate,
        case when b.direct='outcome' then CONVERT(-b.agg_total, FLOAT)   else CONVERT(b.agg_total, FLOAT)  end  as totalAmount,
        '0' autoInvoice,'026' invoiceType,b.counterpart_unit_name buyerName,b.counterpart_unit_code buyerCode,c.taxpayer_identification_number buyerTaxpayerId ,'0'  buyerProperty,
        '91450108MACJKYFT0K'sellerTaxpayerId,'南宁威皓供应链集团有限责任公司'sellerName,
        '1'  includeTaxFlag,'0' pushMatchRules,rec_bill_number textField1
        from  wy_trans_to_kingdee  a,e_account b,e_school_invoice_info c,e_warehouse_org d
        where  a.bill_number=b.bill_number
        and b.counterpart_unit_code=c.school_code
        and  b.warehouse_code=d.wrh_code 
        and  a.bill_type  in  ('dist','salereturn')
        and  a.bill_number=%s
	) a 
    group by billNo,billDate,autoInvoice,invoiceType,buyerName,buyerCode,buyerTaxpayerId,buyerProperty,
    sellerTaxpayerId,sellerName,includeTaxFlag,pushMatchRules,textField1
    """
    exec_sql1 = """
    SELECT  sum(amount) amount,sum(quantity) quantity,detailId,goodsCode,goodsName,lineProperty,price,taxRate
	from (
        SELECT  case when direct='outcome' then CONVERT(-agg_total, FLOAT)   else CONVERT(agg_total, FLOAT) end  as amount,
        bill_number detailId,product_code  goodsCode,product_name goodsName,'2'  lineProperty,CONVERT(product_price, FLOAT)  price,
        case when direct='outcome' then  CONVERT(-product_quantity, FLOAT) else CONVERT(product_quantity, FLOAT) end as  quantity,
        CONVERT(tax_rate, FLOAT) taxRate
        from  e_account
        where  bill_number=%s
	)  a
	group by  detailId,goodsCode,goodsName,lineProperty,price,taxRate
    """
    url = 'http://180.141.91.139:8022/ierp/kapi/app/sim/openApi?access_token='
    key_name = '开票申请单'
    send_messageto_kindee.send_message(key_name, exec_sql, exec_sql1, 4, url)


# 定义定时线程任  每天凌晨5：00将上传表状态为1（上传成功）的订单备份到wy_dist_to_mclz_log中
# def task_wy_dist_to_mclz():
#     send_mesage.task_wy_dist_to_mclz()


# schedule.every(5).minutes.do(run_threaded, task_print, '定义定时心跳，每5分钟执行一次')
# schedule.every(10).minutes.do(run_threaded, task_update2_mclz)
# schedule.every(30).minutes.do(run_threaded, task_send_mclz)

if __name__ == '__main__':
    write_log.write_log(f'明厨亮灶发送数据开始程序执行')
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
    # while True:
    #     # 检查是否有定时任务需要执行
    #     schedule.run_pending()
    #     # 其他代码...
    #     time.sleep(1)
