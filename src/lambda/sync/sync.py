# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import sys
import logging
import json
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

dlq_name = os.environ['DLQ_NAME']

client = boto3.client('kinesis')
sqs = sqs = boto3.resource('sqs')

queue = sqs.get_queue_by_name(QueueName=dlq_name)

def put_record_to_stream(data):
    return client.put_records(
        Records=[
            {
                'Data': data,
                'PartitionKey': 'kinesis'
            },
        ],
        StreamName=os.environ['streamName']
    )

# 除了这里的try except还可以在最外层再加上一层来防止Queue错误，将错误消息和异常输出到cloudwatch log
def lambda_handler(event, context):
    print(event)

    data = json.dumps(event)

    response = ''

    # If DDDDD found as full name, upload data twice as duplicate msg
    if 'NewImage' in event['Records'][0]['dynamodb']:
        try:
            if 'full_name' in event['Records'][0]['dynamodb']['NewImage']:
                if event['Records'][0]['dynamodb']['NewImage']['full_name']['S'] == 'EEEEE':
                    # stream event sourcing doesn't support dead letter queue, only aysnc support?
                    raise Exception("Raised error for testin error handling")

                response = put_record_to_stream(data)
                
                # Check out sink lambda cloud watch log, no error raised
                if event['Records'][0]['dynamodb']['NewImage']['full_name']['S'] == 'DDDDD':
                    print("Duplicate record found and going to insert twice into kinesis stream ===============")
                    response = put_record_to_stream(data)
    
                # Check out IteratorAge	metric on this lambda
                # Emitted for stream-based invocations only (functions triggered by an Amazon DynamoDB stream or 
                # Kinesis stream). Measures the age of the last record for each batch of records processed. 
                # Age is the difference between the time Lambda received the batch, and the time the last record
                # in the batch was written to the stream. Units: Milliseconds
        except Exception as e:
            response = queue.send_message(MessageBody=data)

    print(response)