# ClentData-fetchDataFunction
#
# This lambda function is integrated with the following API methods:
# /customerData [POST] 
#
# its purpose is to fetch data from a URL, encrypts it using KMS and stores it in S3

from __future__ import print_function
import json
import boto3
import urllib.request
import os

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
kms = boto3.client('kms')

def encrypt_data(data, kms_key_id):    
    response = kms.encrypt(
        KeyId=kms_key_id,
        Plaintext=data
    )
    return response['CiphertextBlob']

def get_parameter(parameter_name):
    response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
    return response['Parameter']['Value']

def lambda_handler(event, context):
    # log debug information
    print(event)

    # Extracting the parameters from the event
    url = os.environ['URL_ENDPOINT'] 
    bucket_name = os.environ['BUCKET_NAME']
    object_key = os.environ['OBJECT_KEY']
    kms_key_parameter_name = os.environ['KMS_KEY_PARAMETER_NAME']
    kms_key_id = get_parameter(kms_key_parameter_name)

    # Fetching the data from the URL
    with urllib.request.urlopen(url) as response:
        data = response.read().decode()
        json_data = json.loads(data)

    # Encrypting the data
    encrypted_data = encrypt_data(json.dumps(json_data), kms_key_id)

    # Storing the encrypted data in S3
    s3.put_object(Body=encrypted_data, Bucket=bucket_name, Key=object_key)

    return {
        'statusCode': 200,
        'body': 'Proceso completado exitosamente'
    }