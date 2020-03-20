# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import uuid
import boto3
import datetime

flight_table_name = os.environ['ORDER_TABLE_NAME']
init_db_lambda_name = os.environ['INITDB_LAMBDA_NAME']

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table(flight_table_name)

lam = boto3.client('lambda')


def http_return(body):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': body
    }

def delete_ddb_items():
    # check out https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'customer_id': each['customer_id'],
                }
            )

def load_customer(customer_id):
    resp = table.get_item(
        Key={
            'customer_id': customer_id,
        }
    )

    if 'Item' in resp:
        return resp['Item']
    else:
        return None


def new_order(req):
    order = {}
    order['id'] = str(uuid.uuid4())
    order['flight_number'] = req['flight_number']
    order['flight_date'] = req['flight_date']
    order['from'] = req['from']
    # order['from_airport'] = req['from_airport']
    order['to'] = req['to']
    # order['to_airport'] = req['to_airport']
    order['start'] = req['start']
    order['end'] = req['end']
    order['created_at'] = str(datetime.datetime.now())
    order['status'] = 'Ordered'

    return order


def lambda_handler(event, context):
    print('request: {}'.format(json.dumps(event)))

    path = event["path"]
    method = event["httpMethod"]

    req = {}
    if 'body' in event and event['body'] != None:
        req = json.loads(event['body'])
        print(req)

    # 3 Lambda Invocation Types, both 1/2 works here:
    # 1. RequestResponse – Execute synchronously, no retry.
    # 2. Event – Execute asynchronously, retry 2 times upon failure.
    # 3. DryRun – Test that the invocation is permitted for the caller, but don’t execute the function.
    # API Gateway support both Sync(default)/Async, set integration reqeust http header value: X-Amz-Invocation-Type:Event for async
    # clear DDB and RDS
    if path == "/truncate":
        delete_ddb_items()
        lam.invoke(
            FunctionName=init_db_lambda_name,
            InvocationType='Event',
            Payload=json.dumps( {"TRUNCATE": "y"}))
            
        return http_return('DDB & RDS truncated successfully!\n')
    # create RDS Demo DB
    if path == "/initdb":
        lam.invoke(
            FunctionName=init_db_lambda_name,
            InvocationType='Event',
            Payload=json.dumps( {"KEY1": "y"}))
            
        return http_return('RDS MySQL initialized successfully!\n')

    # Handler customer
    if path == '/':
        # Get customer info along with orders
        if method == 'GET':
            if 'queryStringParameters' in event \
                    and 'customer_id' in event['queryStringParameters']:
                customer_id = event['queryStringParameters']['customer_id']
                # If found return sth like below, otherwise, "Item key not found"
                # "Item": {
                #   "created": "2019-10-19 02:07:50.538833", "customer_id": "23234234234",
                #   "id": "5a9deb22-4b04-47e6-b11e-f6c9e220aa4e", "flight id": "FM1234",
                #   "flight_date": "2019-11-11" }
                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'text/plain'
                    },
                    'body': json.dumps(load_customer(customer_id))
                }
            else:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'text/plain'
                    },
                    'body': "Not Found Customer"
                }
        # Create customer
        elif method == 'POST':
            resp = table.put_item(
                Item={
                    'customer_id': req['customer_id'],
                    'full_name': req['full_name'],
                    'created_at': str(datetime.datetime.now())
                }
            )

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'text/plain'
                },
                'body': json.dumps(resp)
            }
        # Update customer basic information
        elif method == 'PUT':
            resp = table.update_item(
                Key={
                    'customer_id': req['customer_id'],
                },
                UpdateExpression='SET full_name = :val1',
                ExpressionAttributeValues={
                    ':val1': req['full_name']
                }
            )

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'text/plain'
                },
                'body': json.dumps(resp)
            }

    # Handle customer orders
    if path == "/order":
        if method == "GET":
            pass

        # Insert new order to the order list head
        elif method == "POST":
            customer = load_customer(req['customer_id'])

            # Create customer with order on the same time
            if customer == None:
                orders = [new_order(req)]

                resp = table.put_item(
                    Item={
                        'customer_id': req['customer_id'],
                        'orders': json.dumps(orders)
                    }
                )
            # Insert order into customer's order list
            else:
                orders = []
                if 'orders' in customer:
                    orders = json.loads(customer['orders'])

                for order in orders:
                    # Update order status
                    if order['flight_number'] == req['flight_number'] \
                            and order['flight_date'] == req['flight_date'] \
                            and order['status'] == 'Ordered':

                        return {
                            'statusCode': 400,
                            'headers': {
                                'Content-Type': 'text/plain'
                            },
                            'body': 'FLight already ordered'
                        }

                orders.insert(0, new_order(req))

                resp = table.update_item(
                    Key={
                        'customer_id': customer['customer_id'],
                    },
                    UpdateExpression='SET orders = :val1',
                    ExpressionAttributeValues={
                        ':val1': json.dumps(orders)
                    }
                )

            print('response: {}'.format(json.dumps(resp)))

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'text/plain'
                },
                'body': json.dumps(resp)
            }
        # Update order status
        elif method == "PUT":
            customer = load_customer(req['customer_id'])
            if 'orders' in customer:
                orders = json.loads(customer['orders'])
                for order in orders:
                    # Update order status
                    if order['flight_number'] == req['flight_number'] \
                            and order['flight_date'] == req['flight_date']:
                        order['status'] = req['status']
                        order['updated_at'] = str(datetime.datetime.now())
                        resp = table.update_item(
                            Key={
                                'customer_id': customer['customer_id'],
                            },
                            UpdateExpression='SET orders = :val1',
                            ExpressionAttributeValues={
                                ':val1': json.dumps(orders)
                            }
                        )
                        return {
                            'statusCode': 200,
                            'headers': {
                                'Content-Type': 'text/plain'
                            },
                            'body': json.dumps(resp)
                        }
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'text/plain'
                },
                'body': "Order Not Found"
            }
