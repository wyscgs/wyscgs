from flask import Flask, request, jsonify
from execute_sql import *
import time
import json
import threading
import kingdee_api
# import gevent.monkey
import write_log
import wy_token
from webhook import *
import supplierapp_api
from datetime import datetime


# gevent.monkey.patch_all()  # 导入gevent库后需要调用该函数将标准库转换成非阻塞模式

app = Flask(__name__)

# 定义密钥
auth_header_server = 'Rb7Sdlow8w07kahvmH0UpgOUTIluIawj'
kingdee_header_server = 'LhvmH0UpgOURb7Sdlow8waTIluIawj08k'
hd_header_server = 'LhvmH0UpgOQJjrwwe34JFjdkE3DRjk3IluIawj08k'


@app.route("/")
def hi():
    ip = request.remote_addr
    # 获取完整的请求URL
    full_url = request.url
    # 获取主机名（域名）
    hostname = request.host
    # 获取路径
    path = request.path
    write_log.write_log(f"首页:访问ip:{ip} URL:{full_url} 主机名：{hostname} 路径：{path}")
    return "Hello World!"

@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Content-Type'] = 'application/json'  # 设置响应头的 Content-Type 为 JSON
    return response


# 提供给金蝶的接口，获取token by  罗x 20231227
@app.route("/wy_api/get_token")
def get_token():
    ip = request.remote_addr
    write_log.write_log(f"get_token:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != kingdee_header_server:
        return json.dumps({"status": -100, "msg": "访问接口失败"})
    return wy_token.get_token()

#   简道云访问使用，获取订单信息 by 陆国栋 20231225
@app.route("/get_purchase_order", methods=["get"])
def select_purchase():
    ip = request.remote_addr
    write_log.write_log(f"jdy select_purchase:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header == auth_header_server:
        code = request.args.get('code')
        if code is None or code == "":
            return json.dumps({"status": "请输入采购单号！"})
        else:
            result_dic = get_purchase_order(code)
            return json.dumps(result_dic)
    else:
        return json.dumps({"status": 500})

#   简道云访问使用，获取收货信息 by 陆国栋 20231225
@app.route("/get_receipt_order", methods=["get"])
def select_receipt():
    ip = request.remote_addr
    write_log.write_log(f"jdy get_receipt_order:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header == auth_header_server:
        code = request.args.get('code')
        if code is None or code == "":
            return json.dumps({"status": "请输入采购单号！"})
        else:
            result_dic = get_receipt_order(code)
            return json.dumps(result_dic)
    else:
        return json.dumps({"status": 500})


#  简道云访问使用，推送订单信息 by 陆国栋 20231225
@app.route('/callback', methods=['POST'])
def callback():
    ip = request.remote_addr
    write_log.write_log(f"jdy callback:访问ip:{ip}")
    payload = request.data.decode('utf-8')
    nonce = request.args['nonce']
    timestamp = request.args['timestamp']

    secret = ['VE6pInZZw3cgsFMFYbpMCaea', 'NtNLeVpXwBlymauqDs3Fah1M', 'SeSA70Vzcd5ytW2ivvpV7zsM',
              '33oOUYa5J43QEVx9aIOvAiGL']
    for key in secret:
        if request.headers['x-jdy-signature'] == get_signature(nonce, payload, key, timestamp):
            # threading.Thread(target=handle, args=(json.loads(payload),)).start()
            handle(json.loads(payload))
            return 'success'
    return '401'


# 提供给金蝶的接口，返回学校、供应商的信息 by  罗x
# 修改接口增加token_check by ly 20231229
# 定义路由
@app.route("/kingdee/get_customer", methods=["get"])
def get_customer():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"kingdee_api get_customer:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != kingdee_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    customer_type = request.args.get('customer_type')
    page_num = request.args.get('page_num', type=int, default=1)
    if customer_type is None:
        return json.dumps({"status": -5, "msg": "客商类型不能为空"})
    if customer_type.lower() != 'school' and customer_type.lower() != 'supplier':
        return json.dumps({"status": -6, "msg": "客商类型错误:{}".format(customer_type)})
    if not float(page_num) or float(page_num) <= 0:
        return json.dumps({"status": -7, "msg": "查询页参数错误"})
    write_log.write_log(f"kingdee_api:get_customer customer_type=:{customer_type}  page_num={page_num}")
    return kingdee_api.get_customer_list(customer_type, page_num)



# 提供金蝶的接口，返回类别信息 by ly 20231228
# 修改接口增加token_check by ly 20231229
@app.route("/kingdee/get_category", methods=["get"])
def get_category():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"kingdee_api get_category:访问ip:{ip}")
    # 获取Authorization 并验证
    auth_header = request.headers.get('Authorization')
    if auth_header != kingdee_header_server:
        return json.dumps({"status": 500, "msg": "认证失败，请联系系统管理员"})
    #获取token 并验证
    access_token = request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    page_num = request.args.get('page_num', type=int, default=1)
    if not float(page_num) or float(page_num) <= 0:
        return json.dumps({"status": -7, "msg": "查询页参数错误"})
    write_log.write_log(f"kingdee_api:get_category   page_num={page_num}")
    return kingdee_api.get_category_list(page_num)




# 提供金蝶的接口，返回商品信息 by ly 20231228
# 修改接口增加token_check by ly 20231229
@app.route("/kingdee/get_product", methods=["get"])
def get_product():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"kingdee_api get_product:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != kingdee_header_server:
        return json.dumps({"status": 500, "msg": "认证失败，请联系系统管理员"})
    # 获取token 并验证
    access_token = request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取入参
    product_code = request.args.get('product_code')
    page_num = request.args.get('page_num', type=int, default=1)
    if not float(page_num) or float(page_num) <= 0:
        return json.dumps({"status": -7, "msg": "查询页参数错误"})
    if not product_code:
        product_code = None
    else:
        page_num = 1
    print(product_code)
    write_log.write_log(f"kingdee_api:get_product  product_code={product_code} page_num={page_num}")
    return kingdee_api.get_product_list(product_code, page_num)


# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据供应商号获取采购订单号
# by ly 20240113
@app.route("/supplier_app/get_purchase_list", methods=["post"])
def get_purchase_list():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_purchase_list:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        print(access_token)
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取body 中的数据
    body_data = request.get_json()
    b_dates = body_data['begin_date']
    e_dates = body_data['end_date']
    # 提取supplier_code列表中的code值，并将它们转换为包含单引号的字符串列表
    codes = [item['code'] for item in body_data['supplier_code']]
    supplier_codes = [f"'{code}'" for code in codes]
    supplier_codes = ','.join(supplier_codes)
    # print(supplier_codes)
    return supplierapp_api.get_purchase_billnumber_list(b_dates, e_dates,supplier_codes)



# 海鼎数据接口 提供给供应商系统调用 根据供应商号获取所有进销单据
# by ly 20240529
@app.route("/supplier_app/get_bill_list", methods=["post"])
def get_bill_list():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_bill_list:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        print(access_token)
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取body 中的数据
    body_data = request.get_json()
    b_dates = body_data['begin_date']
    e_dates = body_data['end_date']
    page_num = request.args.get('page_num', type=int, default=1)
    # 提取supplier_code列表中的code值，并将它们转换为包含单引号的字符串列表
    codes = [item['code'] for item in body_data['supplier_code']]
    supplier_codes = [f"'{code}'" for code in codes]
    supplier_codes = ','.join(supplier_codes)
    # print(supplier_codes)
    return supplierapp_api.get_bill_list(b_dates, e_dates,supplier_codes,page_num)







# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据单据号获取采购订单单据明细
# by ly 20240113
@app.route("/supplier_app/get_purchase_detail", methods=["post"])
def get_purchase_detail():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_purchase_detail:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    bill_number = request.args.get('bill_number')
    if bill_number is None:
        return json.dumps({"status": -600, "msg": "入参错误，单据号不能为空"})
    return supplierapp_api.get_purchase_detail(bill_number)


# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据单据号获取采购入库单据明细
# by ly 20240114
@app.route("/supplier_app/get_receipt_detail", methods=["post"])
def get_receipt_detail():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_receipt_detail:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    bill_number = request.args.get('bill_number')
    if bill_number is None:
        return json.dumps({"status": -600, "msg": "入参错误，单据号不能为空"})
    return supplierapp_api.get_receipt_detail(bill_number)


# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据单据号获取采购退货单据明细
# by ly 20240117
@app.route("/supplier_app/get_purchase_return_detail", methods=["post"])
def get_purchase_return_detail():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_purchase_return_detail:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    bill_number = request.args.get('bill_number')
    if bill_number is None:
        return json.dumps({"status": -600, "msg": "入参错误，单据号不能为空"})
    return supplierapp_api.get_purchase_return_detail(bill_number)



# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据供应商号获取进货单或者退货单单号列表
# by ly 20240114
@app.route("/supplier_app/get_receipt_list", methods=["post"])
def get_receipt_list():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_receipt_list:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取body 中的数据
    body_data = request.get_json()
    # 提取supplier_code列表中的code值，并将它们转换为包含单引号的字符串列表
    codes = [item['code'] for item in body_data['supplier_code']]
    supplier_codes = [f"'{code}'" for code in codes]
    supplier_codes = ','.join(supplier_codes)
    print(supplier_codes)
    return supplierapp_api.get_receipt_billnumber_list(supplier_codes)


# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据供应商号获取两天后的采购目录
# by ly 20240115
@app.route("/supplier_app/get_product_list", methods=["post"])
def get_product_list():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_product_list:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取body 中的数据
    body_data = request.get_json()
    # 提取supplier_code列表中的code值，并将它们转换为包含单引号的字符串列表
    codes = [item['code'] for item in body_data['supplier_code']]
    supplier_codes = [f"'{code}'" for code in codes]
    supplier_codes = ','.join(supplier_codes)
    print(supplier_codes)
    page_num = request.args.get('page_num', type=int, default=1)
    if not float(page_num) or float(page_num) <= 0:
        return json.dumps({"status": -7, "msg": "查询页参数错误"})
    return supplierapp_api.get_product_list(supplier_codes,page_num)


# 海鼎数据接口 提供给蔬菜公司供应商系统调用 申请批次号
# by lb 20240219
@app.route("/supplier_app/get_batch", methods=["post"])
def get_batch():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_batch:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token = request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取body 中的数据
    wrh_code = request.args.get('wrh_code')
    wrh_name = request.args.get('wrh_name')

    write_log.write_log(f"supplierapp_api:getbatch  wrh_code={wrh_code} wrh_name={wrh_name}")
    return supplierapp_api.get_batch(wrh_code,wrh_name)

# 海鼎数据接口 提供给蔬菜公司供应商系统调用 获取采购订单详情列表
# by lb 20240221
@app.route("/supplier_app/get_purchase_order_line_details", methods=["post"])
def get_purchase_order_line_details():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_purchase_order_line_details:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token = request.args.get('access_token')
    print(access_token)
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取body 中的数据
    lineuuid = request.args.get('lineuuid')

    write_log.write_log(f"supplierapp_api:get_purchase_order_line_details  lineuuid={lineuuid}")
    return supplierapp_api.get_purchase_order_line_details(lineuuid)

# 海鼎数据接口 提供给供应商系统调用,提供当期的采购商品的价格
# by lb 20240221
@app.route("/supplier_app/get_purchase_catalog_price", methods=["post"])
def get_purchase_catalog_price():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_purchase_catalog_price:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token = request.args.get('access_token')
    print(access_token)
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    access_token = request.args.get('access_token')
    # 获取body 中的数据
    product_code = request.args.get('product_code')
    warehouse_code = request.args.get('warehouse_code')
    purchase_date = request.args.get('purchase_date')
    supplier_code= request.args.get('supplier_code')

    write_log.write_log(f"supplierapp_api:get_purchase_catalog_price  product_code={product_code}  "
                        f"warehouse_code={warehouse_code} purchase_date={purchase_date} "
                        f"supplier_code={supplier_code}")
    return supplierapp_api.get_purchase_catalog_price(product_code,warehouse_code,purchase_date,supplier_code)




# 海鼎数据接口 提供给供应商系统调用,新增供应商订单，作废原订单
# by lb 20240221
@app.route("/supplier_app/insert_purchase_order", methods=["post"])
def insert_purchase_order():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app insert_purchase_order:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token = request.args.get('access_token')
    print(access_token)
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})

    # 获取body 中的数据
    purchase_order_info = request.json
    # print(json_data)

    write_log.write_log(f"supplierapp_api:insert_purchase_order  purchase_order_info={purchase_order_info}")
    return supplierapp_api.insert_purchase_order(purchase_order_info)








@app.route("/supplier_app/aborted_purchase_order", methods=["post"])
def aborted_purchase_order():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app aborted_purchase_order:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token = request.args.get('access_token')
    print(access_token)
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})

    # 获取body 中的数据
    purchase_order_info = request.json

    write_log.write_log(f"supplierapp_api:aborted_purchase_order  purchase_order_info={purchase_order_info}")
    return supplierapp_api.aborted_purchase_order(purchase_order_info)



# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据供应商号获取供应商对账单明细
# by ly 20240401
@app.route("/supplier_app/get_supplier_statement", methods=["post"])
def get_supplier_statement():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_supplier_statement:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        print(access_token)
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    # 获取body 中的数据
    body_data = request.get_json()

    # 获取查询字符串中的 'dist_date' 参数
    dist_date_str = request.args.get('dist_date')
    # 尝试将字符串转换为 datetime 对象
    try:
        b_dates = datetime.strptime(dist_date_str, '%Y-%m-%d')  # 假设日期格式是 'YYYY-MM-DD'
    except ValueError:
        # 如果日期格式不正确，设置一个默认值或抛出异常
        return json.dumps({"status": -700, "msg": "日期格式错误"})
    # 提取supplier_code列表中的code值，并将它们转换为包含单引号的字符串列表
    codes = [item['code'] for item in body_data['supplier_code']]
    supplier_codes = [f"'{code}'" for code in codes]
    supplier_codes = ','.join(supplier_codes)
    page_num = request.args.get('page_num', type=int, default=1)
    if not float(page_num) or float(page_num) <= 0:
        return json.dumps({"status": -7, "msg": "查询页参数错误,无页码参数"})
    # print(supplier_codes)
    return supplierapp_api.get_supplier_statement(b_dates,supplier_codes,page_num)


# 海鼎数据接口 提供给蔬菜公司供应商系统调用 根据供应商号获取两天后的采购目录
# by ly 20240115
@app.route("/supplier_app/get_product_bycode", methods=["post"])
def get_product_code():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_product_list:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token =  request.args.get('access_token')
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    product_code = request.args.get('product_code')
    if product_code is None:
        return json.dumps({"status": -600, "msg": "入参错误，商品编号不能为空"})
    return supplierapp_api.get_product_bycode(product_code)




# 海鼎数据接口 提供给供应商系统调用,提供查询时间段内对应供应商号 采购目录的所有有效商品
# by lb 20240710
@app.route("/supplier_app/get_supplier_catalog_product", methods=["post"])
def get_supplier_catalog_product():
    # 获取IP地址
    ip = request.remote_addr
    write_log.write_log(f"supplier_app get_supplier_catalog_product:访问ip:{ip}")
    auth_header = request.headers.get('Authorization')
    if auth_header != hd_header_server:
        return json.dumps({"status": -500, "msg": "认证失败，请联系系统管理员"})
    access_token = request.args.get('access_token')
    print(access_token)
    if not wy_token.check_token(access_token):
        return json.dumps({"status": -600, "msg": "认证失败，token认证失败或者已过有效期"})
    access_token = request.args.get('access_token')
    # 获取body 中的数据
    supplier_info = request.json
    begin_date = supplier_info["begin_date"]
    end_date = supplier_info["end_date"]
    supplier_code =supplier_info["supplier_code"]
    print(f"测试测试：{supplier_code}")
    if supplier_code is None or supplier_code=='':
        write_log.write_log(f"supplierapp_api.get_supplier_catalog_product:supplier_code错误")
        return json.dumps({"status": -100, "msg": "supplier_code为空，请检查后重新输入！"})
    if begin_date is None or begin_date=='':
        write_log.write_log(f"supplierapp_api.get_supplier_catalog_product:begin_date")
        return json.dumps({"status": -100, "msg": "beign_date为空，请检查后重新输入！"})
    if end_date is None or end_date=='':
        write_log.write_log(f"supplierapp_api.get_supplier_catalog_product:end_date错误")
        return json.dumps({"status": -100, "msg": "end_date为空，请检查后重新输入！"})
    write_log.write_log(f"supplierapp_api:get_supplier_catalog_product  begin_date={begin_date}  "
                        f"end_date={end_date} supplier_code={supplier_code} ")
    return supplierapp_api.get_supplier_catalog_product(begin_date,end_date,supplier_code)




if __name__ == "__main__":
    app.run(threaded=True, debug=False, use_reloader=False, host='0.0.0.0', port=8000)

