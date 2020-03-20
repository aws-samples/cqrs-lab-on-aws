# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

#!/usr/bin/env python3
import boto3
import names
import json
import requests
import time
from http import HTTPStatus
from faker import Factory


def query(act):
    if act == "query flight":
        flight = input('Enter the flight naumber: ')

        flight_start = time.time()
        resp = requests.get(EndpointQUERY + '/?flight=' + flight)
        flight_end = time.time()
        resp = resp.content

        flight_delay = flight_end - flight_start

        print(resp)
        print('Customer number is ' + str(len(resp.decode('utf-8').split(']')) - 2))
        print(' (Delay: ' + str(flight_delay) + ')')

    elif act == 'query airport':
        airport = input('Enter the airport (like Guangzhou Baiyun T1): ')

        loc, name, Terminal = airport.split()

        airport_start = time.time()
        resp = requests.get(EndpointQUERY + '/?airport=' + loc + '%20' + name + '%20' + Terminal)
        airport_end = time.time()

        resp = resp.content

        airport_delay = airport_end - airport_start

        print(resp)
        print('Customer number is ' + str(len(resp.decode('utf-8').split(']')) - 2))
        print(' (Delay: ' + str(airport_delay) + ')')

    else:
        customer_start = time.time()
        resp = requests.get(EndpointQUERY + '/?users=y')
        customer_end = time.time()
        resp = resp.content

        customer_delay = customer_end - customer_start

        print(resp)

        print('Total customer number is ' + str(len(resp.decode('utf-8').split(']')) - 2))
        print(' (Delay: ' + str(customer_delay) + ')')


def command(activity):
    if activity == 'initdb':
        initdb()

    elif activity == 'truncatedb':
        truncatedb()

    elif activity == 'register':
        create_customer()

    elif activity == 'put order':
        create_flight()

    elif activity == 'cancel order':
        cancelled_order()

    elif activity == 'reorder':
        order_again()

    elif activity == 'duplicate order':
        duplicate()

    else:
        error_handling()


def initdb():
    initdb_start = time.time()

    resp = requests.post(EndpointInitdb)

    initdb_end = time.time()

    initdb_delay = initdb_end - initdb_start

    print('Init DB!' + ', Delay: ' + str(initdb_delay))

    if resp.status_code != HTTPStatus.OK:
        print(resp.json)


def truncatedb():
    truncate_start = time.time()

    resp = requests.post(EndpointTruncate)

    truncate_end = time.time()

    truncate_delay = truncate_end - truncate_start

    print('Truncate DB!' + ', Delay: ' + str(truncate_delay))

    if resp.status_code != HTTPStatus.OK:
        print(resp.json)


def create_customer():
    global customer_list
    fake = Factory.create('zh_CN')
    for i in range(0, 20):
        customer_id = str(fake.ssn())
        full_name = names.get_full_name()
        data = json.dumps({'customer_id': customer_id,
                           'full_name': full_name})

        create_customer_start = time.time()

        resp = requests.post(EndpointCustomer,
                             data=data)

        create_customer_end = time.time()

        create_customer_delay = create_customer_end - create_customer_start

        customer_list.append(customer_id)

        print('Customer {} registered!'.format(data) + ', Delay: ' + str(create_customer_delay))

        if resp.status_code != HTTPStatus.OK:
            print(resp.json)


def create_flight():
    global cancelled_list, customer_list
    FlightInfo = [
        {'flight_number': 'MU5159', 'from': 'Shanghai Hongqiao T2', 'to': 'Beijing Shoudu T2', 'start': '09:30 PM',
         'end': '11:50 PM', 'flight_date': '2020-01-09'},
        {'flight_number': 'CZ2801', 'from': 'Chongqing Jiangbei T3', 'to': 'Beijing Shoudu T2', 'start': '08:00 AM',
         'end': '10:35 AM', 'flight_date': '2020-01-10'},
        {'flight_number': 'MU2324', 'from': 'Guangzhou Baiyun T1', 'to': 'Xian Xianyang T3', 'start': '06:35 AM',
         'end': '09:05 AM', 'flight_date': '2020-01-13'},
        {'flight_number': 'CZ3243', 'from': 'Chongqing Jiangbei T2', 'to': 'Qingdao Liuting T1', 'start': '08:05 AM',
         'end': '11:05 AM', 'flight_date': '2020-01-14'},
        {'flight_number': 'SC4826', 'from': 'Tianjin Binhai T2', 'to': 'Qingdao Liuting T1', 'start': '10:10 PM',
         'end': '11:25 PM', 'flight_date': '2020-01-15'}]

    for i in range(0, len(customer_list)):

        index = i % 5
        data = json.dumps({'customer_id': customer_list[i],
                           'flight_number': FlightInfo[index]['flight_number'],
                           'from': FlightInfo[index]['from'],
                           'to': FlightInfo[index]['to'],
                           'start': FlightInfo[index]['start'],
                           'end': FlightInfo[index]['end'],
                           'flight_date': FlightInfo[index]['flight_date']})

        create_flight_start = time.time()

        resp = requests.post(EndpointOrder,
                             data=data)

        create_flight_end = time.time()

        create_flight_delay = create_flight_end - create_flight_start

        print('added order: {} for customer: {} '.format(data, customer_list[i]) + ', Delay: ' + str(create_flight_delay))

        if resp.status_code != HTTPStatus.OK:
            print(resp.json)

        if index == 0:
            cancelled_list.append({'customer_id': customer_list[i], 'flight_number': FlightInfo[index]['flight_number'],
                                   'flight_date': FlightInfo[index]['flight_date'], 'from': FlightInfo[index]['from'],
                                   'to': FlightInfo[index]['to'], 'start': FlightInfo[index]['start'],
                                   'end': FlightInfo[index]['end']})

        index_next = (i + 1) % 5
        data = json.dumps({'customer_id': customer_list[i],
                           'flight_number': FlightInfo[index_next]['flight_number'],
                           'from': FlightInfo[index_next]['from'],
                           'to': FlightInfo[index_next]['to'],
                           'start': FlightInfo[index_next]['start'],
                           'end': FlightInfo[index_next]['end'],
                           'flight_date': FlightInfo[index_next]['flight_date']})

        create_flight_start = time.time()

        resp = requests.post(EndpointOrder,
                             data=data)

        create_flight_end = time.time()

        create_flight_delay = create_flight_end - create_flight_start

        print('added order: {} for customer: {} '.format(data, customer_list[i]) + ', Delay: ' + str(create_flight_delay))

        if resp.status_code != HTTPStatus.OK:
            print(resp.json)


def cancelled_order():
    global cancelled_list
    for i in range(0, len(cancelled_list)):
        data = json.dumps({
            'customer_id': cancelled_list[i]['customer_id'],
            'flight_number': cancelled_list[i]['flight_number'],
            'flight_date': cancelled_list[i]['flight_date'],
            'status': 'Cancelled'
        })

        cancelled_order_start = time.time()

        resp = requests.put(EndpointOrder,
                             data=data)

        cancelled_order_end = time.time()

        cancelled_order_delay = cancelled_order_end - cancelled_order_start

        print('cancelled order for customer: ' + cancelled_list[i]['customer_id']
              + ', flight: ' + cancelled_list[i]['flight_number'] + ', flight_date: ' + cancelled_list[i]['flight_date']
              + ', Delay: ' + str(cancelled_order_delay))

        if resp.status_code != HTTPStatus.OK:
            print(resp.json)


def order_again():
    global cancelled_list
    for i in range(0, len(cancelled_list)):
        data = json.dumps({
            'customer_id': cancelled_list[i]['customer_id'],
            'flight_number': cancelled_list[i]['flight_number'],
            'from': cancelled_list[i]['from'],
            'to': cancelled_list[i]['to'],
            'start': cancelled_list[i]['start'],
            'end': cancelled_list[i]['end'],
            'flight_date': cancelled_list[i]['flight_date']
        })

        order_again_start = time.time()

        resp = requests.post(EndpointOrder,
                             data=data)

        order_again_end = time.time()

        order_again_delay = order_again_end - order_again_start

        print('put order for customer: ' + cancelled_list[i]['customer_id']
              + ', flight: ' + cancelled_list[i]['flight_number'] + ', flight_date: ' + cancelled_list[i][
                  'flight_date'] + ', Delay: ' + str(order_again_delay))

        if resp.status_code != HTTPStatus.OK:
            print(resp.json)


def duplicate():
    customer_id = '123456789012345678'
    full_name = 'DDDDD'
    data = json.dumps({'customer_id': customer_id,
                       'full_name': full_name})

    duplicate_start = time.time()

    resp = requests.post(EndpointCustomer,
                         data=data)

    duplicate_end = time.time()

    duplicate_delay = duplicate_end - duplicate_start

    print('Customer {} duplicated!'.format(data) + ', Delay: ' + str(duplicate_delay))

    if resp.status_code != HTTPStatus.OK:
        print(resp.json)


def error_handling():
    customer_id = '098765432109876543'
    full_name = 'EEEEE'
    data = json.dumps({'customer_id': customer_id,
                       'full_name': full_name})

    error_handling_start = time.time()

    resp = requests.post(EndpointCustomer,
                         data=data)

    error_handling_end = time.time()

    error_handling_delay = error_handling_end - error_handling_start

    print('Customer {} error!'.format(data) + ', Delay: ' + str(error_handling_delay))

    if resp.status_code != HTTPStatus.OK:
        print(resp.json)

def get_action():
    action = input('Please enter the action (1. initdb, 2. truncatedb, 3. query customers, 4. query flight, 5. query airport, 6. register, '
                   '7. put order, 8. cancel order, 9. reorder, 10. duplicate order, 11. error handling or 12. exit): ')

    action = action.strip()

    if action == '1':
        action = 'initdb'
    elif action == '2':
        action = 'truncatedb'
    elif action == '3':
        action = 'query customers'
    elif action == '4':
        action = 'query flight'
    elif action == '5':
        action = 'query airport'
    elif action == '6':
        action = 'register'
    elif action == '7':
        action = 'put order'
    elif action == '8':
        action = 'cancel order'
    elif action == '9':
        action = 'reorder'
    elif action == '10':
        action = 'duplicate order'
    elif action == '11':
        action = 'error handling'
    elif action == '12':
        action = 'exit'

    return action

def config_endpoints():
    global EndpointQUERY, EndpointCMD
    try:
        client = boto3.client('cloudformation')
        response = client.describe_stacks(StackName='cqrs')

        for o in response['Stacks'][0]['Outputs']:
            if 'Query' in o['OutputKey']:
                EndpointQUERY = o['OutputValue']
            elif 'Command' in o['OutputKey']:
                EndpointCMD = o['OutputValue']
    except Exception as exp:
        print('Auto set endpoints failed with exception: ' + exp)
        EndpointQUERY = input('Please enter the Query EndPoint:')
        EndpointCMD = input('Please enter the Command EndPoint:')
    else:
        print('Auto set endpoints successfully')

    finally:
        pass

EndpointQUERY = None
EndpointCMD = None

if __name__ == "__main__":
    config_endpoints()

    EndpointCustomer = EndpointCMD
    EndpointOrder = EndpointCMD + '/order'
    EndpointTruncate = EndpointCMD + '/truncate'
    EndpointInitdb = EndpointCMD + '/initdb'

    customer_list = []
    cancelled_list = []

    action = get_action()

    while action != 'exit':
        if action == 'initdb' or action == 'truncatedb' or action == 'register' or action == 'put order' or \
                action == 'cancel order' or action == 'reorder' or action == 'duplicate order' or action == 'error handling':
            command(action)
        elif action == 'query customers' or action == 'query flight' or action == 'query airport':
            query(action)
        else:
            print('Invalid action! Please try again.')

        action = get_action()

    print('Thanks for using our product!')



