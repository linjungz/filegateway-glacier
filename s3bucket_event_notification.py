#A Lambda for processing S3 event log and publish to SNS 
#@Author: Randy Lin

import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    msg = ''
    print(event)
    event_name = event['Records'][0]['eventName']

    if event_name == 'ObjectRestore:Post' :
        msg = 'Object Restore Started.'
    elif event_name == 'ObjectRestore:Completed' :
        msg = 'Object Restore Completed.'
    else:
        msg = 'Unexpected event: ' + event_name

    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    msg = bucket + '/' + key + ' ' + msg

    sns = boto3.client('sns')
    response = sns.publish(
                TopicArn='arn:aws:sns:us-east-2:294254988299:s3bucketread_sns',
                Message=msg,
            )
    print(response)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

