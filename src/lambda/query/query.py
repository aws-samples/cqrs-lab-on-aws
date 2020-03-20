# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import sys
import json
import boto3
import logging
import pymysql

# rds settings
dbName = os.environ["dbName"]
dbUser = os.environ['dbUser']
dbPassword = os.environ['dbPassword']
AuroraEndpoint = os.environ['AuroraEndpoint']

logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
try:
    conn = pymysql.connect(AuroraEndpoint, user=dbUser,
                           passwd=dbPassword, db=dbName, connect_timeout=60)
except:
    logger.error(
        "ERROR: Unexpected error: Could not connect to Aurora instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS Aurora instance succeeded")


def lambda_handler(event, context):

    print('============', event, '===============')
    print(event['queryStringParameters'])

    for k in event['queryStringParameters']:
        if k == 'users':
            sql = "SELECT id, id_number, name, last_updated_at FROM customers"

            with conn.cursor() as cursor:
                cursor.execute('USE {};'.format(dbName))
                cursor.execute(sql)
                results = cursor.fetchall()
                conn.commit()

                print("Total rows: ", len(results))

                for r in results:
                    print(r)
                return {
                    'statusCode': 200,
                    'headers': {
                        "Access-Control-Allow-Origin": "*"
                    },
                    'body': json.dumps(results)
                }


        if k == 'flight':
            flight = event['queryStringParameters'][k]
            print('Flight:', len(flight))
            print('Flight:', flight)
            #flight = 'CZ3243'
            # MySQL table name is case sensitive
            # 更改或者取消某个指定航班，需要查询出所有订阅此航班的人乘
            sql = "SELECT name,mobile FROM customers c, orders o WHERE c.id = o.customer_id and o.STATUS='Ordered' and o.FLIGHT_NUMBER='{}'".format(
                flight)
                
            print(sql) 

            with conn.cursor() as cursor:
                cursor.execute('USE {};'.format(dbName))
                cursor.execute(sql)
                results = cursor.fetchall()
                conn.commit()

                print("Total rows: ", len(results))

                for r in results:
                    print(r)
                return {
                    'statusCode': 200,
                    'headers': {
                        "Access-Control-Allow-Origin": "*"
                    },
                    'body': json.dumps(results)
                }

        if k == 'airport':
            airport = event['queryStringParameters'][k]
            # 机场警告通知，需要查询出所有飞经此机场的所有航班，从而通知到每个乘客
            sql = "SELECT name,mobile FROM customers c, orders o WHERE c.id = o.customer_id and o.STATUS='Ordered' and (o.FROM_CITY='{}' or o.TO_CITY='{}')".format(
                airport, airport)

            with conn.cursor() as cursor:
                cursor.execute('USE {};'.format(dbName))
                cursor.execute(sql)
                results = cursor.fetchall()
                conn.commit()

                print("Total rows: ", len(results))

                for r in results:
                    print(r)
                return {
                    'statusCode': 200,
                    'headers': {
                        "Access-Control-Allow-Origin": "*"
                    },
                    'body': json.dumps(results)
                }
