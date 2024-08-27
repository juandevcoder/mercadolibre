# ClentData-ProtectPIIDataFunction
#
# This lambda function is integrated with Amazon s3:
#
#
# its purpose is to generate a new file with masked PCI data and store it in S3

from __future__ import print_function
import boto3
import json
import os
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
ssm = boto3.client('ssm')
kms = boto3.client('kms', region_name='us-west-2')

def get_parameter(parameter_name):
    response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
    return response['Parameter']['Value']

def mask_credit_card_number(credit_card_number):
    return credit_card_number[:4] + '*' * (len(credit_card_number) - 8) + credit_card_number[-4:]

def mask_account_number(account_number):
    return account_number[:2] + '*' * (len(account_number) - 4) + account_number[-2:]

def lambda_handler(event, context):
    # log debug information
    print(event)

    #Extract parameters from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    new_object_key = os.environ['NEW_OBJECT_KEY']
    kms_key_parameter_name = os.environ['KMS_KEY_PARAMETER_NAME']
    kms_key_id = get_parameter(kms_key_parameter_name)

    try:
        # Get the encrypted file from S3
        encrypted_file = s3.get_object(Bucket=bucket_name, Key=object_key)
        encrypted_data = encrypted_file['Body'].read().decode('utf-8')

        # Decrypt the file using KMS
        decrypted_data = kms.decrypt(CiphertextBlob=encrypted_data, KeyId=kms_key_id)['Plaintext'].decode('utf-8')
        
        # Mask PCI data from the file
        data = json.loads(decrypted_data)
        for item in data:
            item['credit_card_num'] = mask_credit_card_number(item['credit_card_num'])
            item['credit_card_ccv'] = '***'
            item['cuenta_numero'] = mask_account_number(item['cuenta_numero'])

        # Create a new JSON file
        new_file = json.dumps(data)

        # Encrypt the new file using KMS
        encrypted_new_file = kms.encrypt(Plaintext=new_file.encode('utf-8'), KeyId=kms_key_id)['CiphertextBlob']

        # Store the new file in S3
        s3.put_object(Bucket=bucket_name, Key=new_object_key, Body=encrypted_new_file)

        return {
            'statusCode': 200,
            'body': 'Proceso completado exitosamente'
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': f'Error al procesar el archivo cifrado: {e}'
        }
