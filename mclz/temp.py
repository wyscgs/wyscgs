from get_token import get_token
import json
import requests
import dbconnection as dbcon
import write_log


def select_distno():
    bill_number = []
    sql_str = """
        select bill_number from wy_dist_to_mclz
        where  state='2'
        limit 1
        """
    # 连接数据库
    try:
        cnx = dbcon.db_connect()
        if cnx is None:
            write_log.write_log('select_distno连接数据库失败')
            return False
        else:
            cursor = dbcon.exec_sql(cnx, sql_str)
            if cursor is None:
                write_log.write_log('select_distno执行语句失败')
                return False
            else:
                results = cursor.fetchall()
                for row in results:
                    bill_number.append(row[0])
                return bill_number
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def make_json_to_mclz():
    dist_billnumber = select_distno()
    for billnumber in dist_billnumber:
        send_data = {
            "data": {
                "url": "AddFoodfirmfillin",
                "paramdata": {
                    "FD_FoodfirmfillinfileInfos": [{
                        "FileTypeId": 1,
                        "FileName": "",
                        "FileUrl": ""
                    }],
                    "FD_FoodfirmfillindetailInfos": []
                }
            }
        }
        try:
            cnx = dbcon.db_connect()
            cnx2 = dbcon.db_connect()
            cnx3 = dbcon.db_connect()
            if cnx is None or cnx2 is None or cnx3 is None:
                write_log.write_log('接数据库失败')
                return False
            else:
                # 获取订单信息
                exec_sql = """
                select  a.bill_number DistNo, '南宁威耀集采配供链管理有限公司'  DistSupplyName,DATE_FORMAT(b.dist_Date, '%Y-%m-%d')  DistDate,
                '5685930500872052978'  FirmID,d.contact_name  PurchaseMan,d.contact_name  CheckMan ,
                DATE_FORMAT(b.dist_Date, '%Y-%m-%d %H:%i:%S')  FillInDatetime
                from  wy_dist_to_mclz a ,e_dist b ,e_school d
                WHERE  a.bill_number=b.bill_number
                and   b.school_code=d.code 
                and   a.state='2'
		        and   a.bill_number='{}'
                """.format(billnumber)
                cursor = dbcon.exec_sql(cnx, exec_sql)
                # 获取订单的商品详情信息
                exec_sql2 = """
		        select  CONVERT(c.plan_quantity, FLOAT) OrderQuantity, CONVERT(c.quantity, FLOAT)  DeliveryQuantity,CONVERT(c.receive_quantity, FLOAT)   CheckQuantity,
                CONVERT(c.price, FLOAT) Price,MAX(DATE_FORMAT(f.production_date, '%Y-%m-%d %H:%i:%S'))   ProductDate,MAX(f.batch)  ProductStandardCode,
                h.code  GenericCode,h.name  GenericName,h.category_code  FoodTypeCode,h.category_name FoodTypeName, h.specification Specification,
				h.spec_unit Unit,h.shelf_life_value  ShelfLife,MAX(f.supplier_name)  ProductFirmName,
				MAX(CONCAT('https://erp.nnwysc.com/eos-download/accessory',g.file_path)) ProductImageUrl
                from   e_dist b,e_dist_line c ,e_dist_line_detail e  ,e_batch  f,e_product h,e_accessory g
                WHERE  b.bill_number=c.dist_bill_number
                and   c.uuid=e.dist_line_uuid
                and   e.batch=f.batch
                and   b.warehouse_code=f.wrh_code
                and   c.product_code=h.code
				and   h.uuid=g.owner_uuid
				and   b.bill_number='{}'
				group by  c.plan_quantity,c.quantity,c.receive_quantity ,c.price ,h.code  ,h.name  ,
				h.category_code  ,h.category_name , h.specification ,h.spec_unit ,h.shelf_life_value  
                """.format(billnumber)
                cursor2 = dbcon.exec_sql(cnx2, exec_sql2)

                if cursor is None or cursor2 is None:
                    write_log.write_log('cursor或者cursor2执行语句失败')
                    return False
                else:
                    # 拼接配送订单信息
                    results = cursor.fetchall()
                    ls = [description[0] for description in cursor.description]  # 获取列名
                    for row in results:
                        for i in range(len(ls)):
                            send_data["data"]["paramdata"][ls[i]] = row[i]
                    # 拼接配送订单的商品信息
                    results2 = cursor2.fetchall()
                    ls2 = [description[0] for description in cursor2.description]  # 获取列名
                    for row2 in results2:
                        product_info = {
                            "FD_FoodInfo": {

                            }
                        }
                        for i in range(len(ls2)):
                            if (ls2[i] == "OrderQuantity" or ls2[i] == "DeliveryQuantity" or ls2[i] == "CheckQuantity"
                                    or ls2[i] == "Price" or ls2[i] == "ProductDate" or ls2[i] == "ProductStandardCode"):
                                product_info[ls2[i]] = row2[i]
                                if ls2[i] == "ProductStandardCode":
                                    exec_sql3 = """
                                    select  j.name FileName,CONCAT('https://erp.nnwysc.com/eos-download/accessory',j.file_path) FileUrl
                                     from   e_batch h,e_accessory j
                                     WHERE h.batch=j.owner_uuid
                                     and   h.batch='{}'
                                     """.format(row2[i])
                                    cursor3 = dbcon.exec_sql(cnx3, exec_sql3)
                                    if cursor3 is None:
                                        write_log.write_log('cursor3执行语句失败')
                                        return False
                                    else:
                                        # 拼接配送订单信息
                                        results3 = cursor3.fetchall()
                                        file_temp = []
                                        ls3 = [description[0] for description in cursor3.description]  # 获取列名
                                        for row3 in results3:
                                            temp = {}
                                            for k in range(len(ls3)):
                                                temp[ls3[k]] = row3[k]
                                            file_temp.append(temp)
                                        product_info["FD_FoodfirmfillindtlfileInfos"] = file_temp
                            else:
                                product_info["FD_FoodInfo"][ls2[i]] = row2[i]
                        send_data["data"]["paramdata"]["FD_FoodfirmfillindetailInfos"].append(product_info)
                json_data = json.dumps(send_data, ensure_ascii=False, indent=2)
                return json_data

        except Exception as e:
            print(f"An error occurred: {str(e)}")


def test():
    json_data2 = {
        "data": {
            "url": "AddFoodfirmfillin",
            "paramdata": {
                "FD_FoodfirmfillinfileInfos": [
                    {
                        "FileTypeId": 1,
                        "FileName": "",
                        "FileUrl": ""
                    }
                ],
                "FD_FoodfirmfillindetailInfos": [
                    {
                        "FD_FoodInfo": {
                            "GenericCode": "000865",
                            "GenericName": "鸡翅根（冷鲜）1kg（约16-20个/kg）",
                            "FoodTypeCode": "0103",
                            "FoodTypeName": "禽类",
                            "Specification": "1kg",
                            "Unit": "袋",
                            "ShelfLife": 3,
                            "ProductFirmName": "广西春江供应链有限公司",
                            "ProductImageUrl": "https://erp.nnwysc.com/eos-download/accessory/product/2021/12/02/0001/fce92313-9892-47d8-91e6-f39ad0753b4b.jpg"
                        },
                        "OrderQuantity": 1.0,
                        "DeliveryQuantity": 1.0,
                        "CheckQuantity": 1.0,
                        "Price": 23.0,
                        "ProductDate": "2023-12-14 00:00:00",
                        "ProductStandardCode": "20231213001014",
                        "FD_FoodfirmfillindtlfileInfos": [
                            {
                                "FileName": "鸡分割.jpg",
                                "FileUrl": "https://erp.nnwysc.com/eos-download/accessory/batch/CR20/23/12/CR202312140289/1d87970a-5b8b-4e7f-9a55-f76fe567e974.jpg"
                            }
                        ]
                    },
                    {
                        "FD_FoodInfo": {
                            "GenericCode": "01060029",
                            "GenericName": "炸豆腐果（大）500g",
                            "FoodTypeCode": "0106",
                            "FoodTypeName": "半加工菜",
                            "Specification": "500g",
                            "Unit": "袋",
                            "ShelfLife": 3,
                            "ProductFirmName": "广西横县丰润园食品有限公司",
                            "ProductImageUrl": "https://erp.nnwysc.com/eos-download/accessory/product/2022/06/15/0001/5e152749-84c5-4ac0-8172-5baaf4c19ebe.jpg"
                        },
                        "OrderQuantity": 2.0,
                        "DeliveryQuantity": 2.0,
                        "CheckQuantity": 2.0,
                        "Price": 7.2,
                        "ProductDate": "2023-12-14 00:00:00",
                        "ProductStandardCode": "20231214000552",
                        "FD_FoodfirmfillindtlfileInfos": [
                            {
                                "FileName": "豆腐果大500.jpg",
                                "FileUrl": "https://erp.nnwysc.com/eos-download/accessory/batch/CR20/23/12/CR202312140327/76655819-0cdd-4992-b654-5f1e76892793.jpg"
                            }
                        ]
                    }
                ],
                "DistNo": "XS20231215003644",
                "DistSupplyName": "南宁威耀集采配供链管理有限公司",
                "DistDate": "2023-12-15",
                "FirmID": "5685930500872052978",
                "PurchaseMan": "闭克环",
                "CheckMan": "闭克环",
                "FillInDatetime": "2023-12-15 00:00:00"
            }
        }
    }
    return json_data2


if __name__ == '__main__':
    token = get_token()

    # 定义要发送的 JSON 数据
    json_data = make_json_to_mclz()
    print(json_data)
    # 定义请求头中的 token
    headers = {
        "token": token,
        "Content-Type": "application/json"
    }

    # 定义目标 URL
    url = "http://36.137.36.34:9015/v1/Third/ThirdRequest"
