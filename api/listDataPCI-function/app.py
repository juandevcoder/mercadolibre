# ClentData-listDataPCIFunction
#
# This lambda function is integrated with the following API methods:
# /customerData/data [GET] 
#
# its purpose is to get customer data with masked PCI and not masked PII

from __future__ import print_function
import boto3
import os
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    # log debug information
    print(event)
    
    # Extract parameters from the event
    table_name = os.environ['TABLE_NAME']

    # Get the table
    table = dynamodb.Table(table_name)

    # Get the data from the table
    if 'user_name' in event:
        response = table.query(
            KeyConditionExpression=Key('user_name').eq(event['user_name'])
        )
    else:
        # Scan the table
        response = table.scan()

    items = response['Items']
    return items
