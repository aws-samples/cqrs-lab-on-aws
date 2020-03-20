# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import sys
import logging
import pymysql

# rds settings
dbName = os.environ['dbName']
dbUser = os.environ['dbUser']
dbPassword = os.environ['dbPassword']
AuroraEndpoint = os.environ['AuroraEndpoint']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        print("%s - %s - %s" % (dbUser, dbPassword, AuroraEndpoint))
        #connection = pymysql.connect(AuroraEndpoint, user=dbUser, passwd=dbPassword, db=dbName, connect_timeout=5)
        connection = pymysql.connect(host=AuroraEndpoint,
                                 user=dbUser,
                                 password=dbPassword,
                                 #db=dbName,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{}'".format(dbName)
                cursor.execute(sql)
                results = cursor.fetchall()
                connection.commit()
            
                # Not exist or force recreation
                if len(results) == 1 and 'DROP' not in event.keys():
                    if 'TRUNCATE' in event.keys():
                        with connection.cursor() as cursor:
                            print('as===========')
                            cursor.execute('USE {};'.format(dbName))
                            cursor.execute('delete from orders;')
                            cursor.execute('delete from customers;')
                            connection.commit()
                    else:
                        print("DB {} already exist, do nothing".format(dbName))
                else:
                    # Drop DB
                    if len(results) > 0 and 'DROP' in event.keys():
                        print("Drop existing DB and recreating")
                    else:
                        print("Create new database from nothing")

                    cursor.execute('DROP DATABASE IF EXISTS {}'.format(dbName))
    
                    cursor.execute('CREATE DATABASE {}'.format(dbName))
                    cursor.execute('USE {};'.format(dbName))
                    connection.commit()
                    
                    print("DB Created!")
                    
                    # Create customer table
                    sql = "CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, id_number VARCHAR(20) NOT NULL UNIQUE, \
                            name VARCHAR(255), mobile VARCHAR(20), address VARCHAR(255), \
                            last_updated_at VARCHAR(255))"
            
                    cursor.execute(sql)
                    # Create order table, from & to are reservered keywords for MySQL table, do NOT use.
                    sql = "CREATE TABLE orders (id INT AUTO_INCREMENT PRIMARY KEY, \
                                customer_id INT NOT NULL, \
                                flight_number VARCHAR(8) NOT NULL, \
                                flight_date VARCHAR(10) NOT NULL, \
                                from_city VARCHAR(20) NOT NULL, \
                                to_city VARCHAR(20) NOT NULL,  \
                                status VARCHAR(20) NOT NULL,  \
                                serial_no VARCHAR(64) NOT NULL, \
                                FOREIGN KEY (customer_id) REFERENCES customers(id) )"
                    cursor.execute(sql)
                    connection.commit()
                    
                    print("Table Created!")

                    # sqls = [ "INSERT INTO customers (id, id_number, name, mobile, address) VALUES (1, '123456781', 'David Tian 1', '11111111111', 'anywhere1')",
                    #         "INSERT INTO customers (id, id_number, name, mobile, address) VALUES (2, '123456782', 'David Tian 2', '11111111112', 'anywhere2')",
                    #         "INSERT INTO customers (id, id_number, name, mobile, address) VALUES (3, '123456783', 'David Tian 3', '11111111113', 'anywhere3')",
                    #         "INSERT INTO customers (id, id_number, name, mobile, address) VALUES (4, '123456784', 'David Tian 4', '11111111114', 'anywhere4')",
                    #         "INSERT INTO orders (customer_id, flight_number, flight_date, from_city, to_city, status, serial_no)  \
                    #                             VALUES (1, 'FM12345', '2019-11-10', 'Beijing', 'Shanghai', 'Cancelled', 'd6082542-19aa-4d6d-b376-3fcc50c30f06')",
                    #         "INSERT INTO orders (customer_id, flight_number, flight_date, from_city, to_city, status, serial_no)  \
                    #                             VALUES (1, 'FM12345', '2019-11-13', 'Beijing', 'Shanghai', 'Ordered', 'd6082542-19aa-4d6d-b376-3fcc50c30f07')",
                    #         "INSERT INTO orders (customer_id, flight_number, flight_date, from_city, to_city, status, serial_no)  \
                    #                             VALUES (1, 'FM12345', '2019-11-11', 'Shanghai', 'Beijing', 'Ordered', 'd6082542-19aa-4d6d-b376-3fcc50c30f08')",
                    #         "INSERT INTO orders (customer_id, flight_number, flight_date, from_city, to_city, status, serial_no)  \
                    #                             VALUES (1, 'FM12346', '2019-11-11', 'Beijing', 'Shanghai', 'Ordered', 'd6082542-19aa-4d6d-b376-3fcc50c30f09')",
                    #         "INSERT INTO orders (customer_id, flight_number, flight_date, from_city, to_city, status, serial_no)  \
                    #                             VALUES (2, 'FM12345', '2019-11-10', 'Beijing', 'Shanghai', 'Cancelled', 'd6082542-19aa-4d6d-b376-3fcc50c30f10')" ]
                    # for sql in sqls:
                    #     print(sql)
                    #     cursor.execute(sql)
                    # connection.commit()

                    # print("Sample data created!")

        except Exception as e:
            print("Exception {} in init DB!".format(e))
        finally:
            connection.close()

    except:
        logger.error("ERROR: Unexpected error: Could not connect to DB instance.")
        sys.exit()
