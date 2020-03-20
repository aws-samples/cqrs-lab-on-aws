import os
import sys
import logging
import pymysql
import json
import datetime
import base64

# rds settings
dbName = os.environ['dbName']
dbUser = os.environ['dbUser']
dbPassword = os.environ['dbPassword']
AuroraEndpoint = os.environ['AuroraEndpoint']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    print("%s - %s - %s" % (dbUser, dbPassword, AuroraEndpoint))
    connection = pymysql.connect(host=AuroraEndpoint,
                                 user=dbUser,
                                 password=dbPassword,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
except:
    print("Exception in connect!")


def lambda_handler(event, context):
    print(event)

    for record in event['Records']:
        #Kinesis data is base64 encoded so decode here
        payload=base64.b64decode(record["kinesis"]["data"])
        
        payload_str = payload.decode('utf8')
        payload_json = json.loads(payload_str)
        print("Decoded payload: " + payload_str)
        
        for mainInfo in payload_json['Records']:
            order_type = mainInfo['eventName']
            print(order_type)
        
            try:
                with connection.cursor() as cursor:
                    # 连接数据库
                    cursor.execute('USE {};'.format(dbName))
                    # customer_id
                    id_number = mainInfo['dynamodb']['Keys']['customer_id']['S']
    
                    if order_type == 'INSERT':
                        # full_name
                        name = mainInfo['dynamodb']['NewImage']['full_name']['S']
                        # mobile=
                        # address=
                        print(id_number, name)
                        # 1.插入顾客信息，并忽略重复插入
                        sql = "INSERT IGNORE INTO customers (id_number, name, mobile, address, last_updated_at) VALUES ('{}','{}', '11111111111', 'anywhere1', '{}')".format(
                            id_number, name, str(datetime.datetime.now()))
                        cursor.execute(sql)
                        connection.commit()
                    if order_type == 'MODIFY':
                        # full_name
                        new_name = mainInfo['dynamodb']['NewImage']['full_name']['S']
                        old_name = mainInfo['dynamodb']['OldImage']['full_name']['S']
                        # 2.用户名变更
                        if new_name != old_name:
                            sql = "update customers set name='{}', last_updated_at='{}' where id_number='{}'".format(
                                new_name, id_number, str(datetime.datetime.now()))
                            cursor.execute(sql)
                            connection.commit()
                        # 订单状态变更
                        else:
                            orderInfo = json.loads(
                                mainInfo['dynamodb']['NewImage']['orders']['S'])
                            print(orderInfo)
                            order_id = []
                            for i in orderInfo:
                                order_id.append(i['id'])
                            for i in range(len(order_id)):
                                sql = "select * from orders where serial_no in ('{}')".format(
                                    order_id[i])
                                print(sql)
                                x = cursor.execute(sql)
                                print(x)
    
                                serial_no = orderInfo[i]['id']
                                status = orderInfo[i]['status']
                                if x == 0:
                                    # 3.新增一个order
                                    flight_number = orderInfo[i]['flight_number']
                                    flight_date = orderInfo[i]['flight_date']
                                    from_city = orderInfo[i]['from']
                                    to_city = orderInfo[i]['to']
    
                                    # 从customers表中提取出id
                                    sql_extract_id_from_customers = "select id from customers where id_number='{}'".format(
                                        id_number)
                                    cursor.execute(sql_extract_id_from_customers)
                                    id_from_customers = cursor.fetchall()[
                                        0].get('id')
    
                                    # 执行插入orders表sql语句，其中customer_id为customers表中的id，并忽略重复插入
                                    sql_order = "INSERT IGNORE INTO orders (customer_id, flight_number, flight_date, from_city, to_city, status, serial_no) VALUES ('{}','{}', '{}', '{}','{}','{}','{}')".format(
                                        id_from_customers, flight_number, flight_date, from_city, to_city, status, serial_no)
                                    print(sql_order)
                                    cursor.execute(sql_order)
                                    connection.commit()
    
                                # 4.order状态变更
                                else:
                                    sql = "update orders set status='{}' where serial_no='{}'".format(
                                        status, serial_no)
                                    cursor.execute(sql)
                                    connection.commit()
    
            except Exception as e:
                print("Exception {} in sync to RDS MySQL DB!".format(e))
