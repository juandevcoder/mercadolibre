import boto3
import os
import json
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
ssm = boto3.client('ssm')
dynamodb = boto3.client('dynamodb')
kms = boto3.client('kms')

def get_parameter(parameter_name):
    response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
    return response['Parameter']['Value']

def lambda_handler(event, context):
    # log debug information
    print(event)

    #Extract parameters from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    kms_key_parameter_name = os.environ['KMS_KEY_PARAMETER_NAME']
    kms_key_id = get_parameter(kms_key_parameter_name)
    table_name = 'TABLE_NAME'

    try:
        # Get the encrypted file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        encrypted_data = response['Body'].read()

        # Decrypt the file using KMS
        response = kms.decrypt(CiphertextBlob=encrypted_data, KeyId=kms_key_id)
        decrypted_data = response['Plaintext'].decode('utf-8')

        # Store the decrypted data in DynamoDB
        items = json.loads(decrypted_data)
        for item in items:
            dynamodb.put_item(TableName=table_name, Item=item)

        return {
            'statusCode': 200,
            'body': 'Datos almacenados en DynamoDB exitosamente'
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': f'Error al procesar el archivo cifrado: {e}'
        }
