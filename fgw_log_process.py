#A Lambda for processing file gateway health logs and restore object respectively
#@Author: Randy Lin

import gzip
import json
import base64
import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    # TODO implement
    print(event)
    cw_data = event['awslogs']['data']
    compressed_payload = base64.b64decode(cw_data)
    uncompressed_payload = gzip.decompress(compressed_payload)
    payload = json.loads(uncompressed_payload)
    logEvents = payload['logEvents']
    
    print(logEvents)
    
    for logEvent in logEvents:
        print(logEvent)
        print(logEvent['message'])
        message_payload = json.loads(logEvent['message'])
        print(message_payload)
        msg_type = message_payload['type']
        msg_key = message_payload['key']
        msg_bucket = message_payload['bucket']

        if message_payload['type'] == 'InaccessibleStorageClass' :
            print('Got InaccessibleStorageClass Erorr')
            #start restore object from gracier/da
            #TODO: add human approval process for restoration
  
            # Restore archived object for two days. Expedite the restoration.
            success = restore_object(msg_bucket, msg_key, 1, 'Expedited')
            if success:
                print('Request submitted.')

    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


#restore object
def restore_object(bucket_name, object_name, days, retrieval_type='Standard'):
    """Restore an archived S3 Glacier/DA object in an Amazon S3 bucket

    :param bucket_name: string
    :param object_name: string
    :param days: number of days to retain restored object
    :param retrieval_type: 'Standard' | 'Expedited' | 'Bulk'
    :return: True if a request to restore archived object was submitted, otherwise
    False
    """

    # Create request to restore object
    request = {'Days': days,
               'GlacierJobParameters': {'Tier': retrieval_type}}

    # Submit the request
    s3 = boto3.client('s3')
    try:
        s3.restore_object(Bucket=bucket_name, Key=object_name, RestoreRequest=request)
    except ClientError as e:
        # NoSuchBucket, NoSuchKey, or InvalidObjectState error == the object's
        # storage class was not GLACIER
        print(e)
        return False
    return True
            
