import dbconnection as dbcon
import write_log


def send_message_to_mclz():
    # 连接数据库
    exec_sql = """
    SELECT  t.*,h.code  GenericCode,h.name  GenericName,h.category_code  FoodTypeCode,category_name FoodTypeName, h.specification Specification,h.spec_unit Unit,
    h.shelf_life_value  ShelfLife,j.file_path ProductImageUrl
    from 
    (
    select  a.bill_number DistNo, '南宁威耀集采配供链管理有限公司'  DistSupply,b.dist_Date  DistDate,d.salesman  SupplyMan,d.food_safe_account  FirmID,
    d.contact_name  PurchaseMan,d.contact_name  CheckMan , b.dist_Date FillInDatetime, c.plan_quantity OrderQuantity,c.quantity DeliveryQuantity,c.receive_quantity  CheckQuantity,
    c.price Price,f.production_date  ProductDate,f.batch  ProductStandardCode,g.name  FileName,g.file_path  FileUrl,c.product_code,f.supplier_name  ProductFirmName
    from  wy_dist_to_mclz a ,e_dist b,e_dist_line c,e_school d ,e_dist_line_detail e  ,e_batch  f,e_accessory g
    WHERE  a.bill_number=b.bill_number
    and   a.bill_number=c.dist_bill_number
    and   b.school_code=d.code 
    and   c.uuid=e.dist_line_uuid
    and   e.batch=f.batch
    and   f.batch=g.owner_uuid
    and a.state='2'
    ) t,e_product h,e_accessory j
    where  t.product_code=h.code
    and  h.uuid=j.owner_uuid
    limit 1
        """
    cnx = dbcon.db_connect()
    if cnx is None:
        write_log.write_log('连接数据库失败,定时任务退出')
        return False
    # 执行查询出来的语句
    cursor = dbcon.exec_sql(cnx, exec_sql)
    if cursor is None:
        write_log.write_log('执行SQL语句失败,定时任务退出')
        dbcon.db_disconnect(cnx)
        return False
    results = cursor.fetchall()
    ls = [description[0] for description in cursor.description]
    # print(ls)
    # 打印结果
    ls_2 = []
    for row in results:
        temp = {}
        for i in range(len(row)):
            temp[ls[i]] = row[i]
        ls_2.append(temp)
    # print(ls_2)
    for i in range(len(ls_2)):
        send_data = {}
        data_temp = {"url": "AddFoodfirmfillin"}
        paramdata_temp = {}
        detailInfos_temp = {}
        fileInfos_temp = {}
        FoodInfo_temp = {}
        FD_FoodfirmfillindtlfileInfos = []
        FD_FoodfirmfillindetailInfos = []
        for key in ls_2[i]:
            if (key == "DistNo" or key == "DistSupply" or key == "DistDate" or key == "SupplyMan" or key == "FirmID"
                    or key == "PurchaseMan" or key == "CheckMan" or key == "FillInDatetime"):
                paramdata_temp[key] = ls_2[i][key]
            elif (key == "OrderQuantity" or key == "DeliveryQuantity" or key == "CheckQuantity" or key == "Price" or
                  key == "ProductDate" or key == "ProductStandardCode"):
                detailInfos_temp[key] = ls_2[i][key]
            elif key == "FileName" or key == "FileUrl":
                fileInfos_temp[key] = ls_2[i][key]
            else:
                FoodInfo_temp[key] = ls_2[i][key]
        # 将FoodInfo_temp字典放入FD_FoodfirmfillindtlfileInfos列表
        FD_FoodfirmfillindtlfileInfos.append(fileInfos_temp)
        # 将FoodInfo_temp放入detailInfos_temp的字典，键值为FD_FoodInfo
        detailInfos_temp["FD_FoodInfo"] = FoodInfo_temp
        # 将列表FD_FoodfirmfillindtlfileInfos放入detailInfos_temp字典，FD_FoodfirmfillindtlfileInfos
        detailInfos_temp["FD_FoodfirmfillindtlfileInfos"] = FD_FoodfirmfillindtlfileInfos
        # 将detailInfos_temp放入FD_FoodfirmfillindetailInfos列表
        FD_FoodfirmfillindetailInfos.append(detailInfos_temp)
        # 将FD_FoodfirmfillindetailInfos列表放入paramdata_temp字典中，键值为FD_FoodfirmfillindetailInfos
        paramdata_temp["FD_FoodfirmfillindetailInfos"] = FD_FoodfirmfillindetailInfos
        # 将paramdata_temp放入data字典中，键值为paramdata
        data_temp["paramdata"] = paramdata_temp
        # 将data_temp放入send_data中，键值为data
        send_data["data"] = data_temp
        print(send_data)
    # 断开数据库连接
    dbcon.db_disconnect(cnx)


if __name__ == '__main__':
    send_message_to_mclz()
