# restore.py
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function

import json
import boto3
from io import BytesIO
from botocore.exceptions import ClientError
from botocore.client import Config

def lambda_handler(event, context):
    
    print("event")
    print(event)
    print("context")
    print(context)

    # Check if this is a valid retrieval request
    sns_message = event['Records'][0]['Sns']['Message']
    sns_check_message = json.loads(sns_message)
    if sns_check_message['Action'] != 'ArchiveRetrieval' and sns_check_message['StatusCode'] != 'Succeeded':
        print('Invalid archival retrieval request!')
        return {
            'statusCode': 403,
            'body': json.dumps('Forbidden: Invalid archival request!')
        }
    
    # Set up parameters
    region_name = "us-east-1"
    signature_version = "s3v4"
    glacier_vault_name = "ucmpcs"
    user_name = "brianchan"
    hw = "16"
    sqs_restore = (
        f"https://sqs.us-east-1.amazonaws.com/127134666975/{user_name}_a{hw}_job_restore"
    )
    sqs_wait_time = 20
    sqs_max_messages = 10
    s3_results_bucket = "gas-results"
    table_name = f"{user_name}_annotations"
    
    # Get job id and archive id from SQS
    
    # Open a connection to sqs
    sqs_client = boto3.client(
        "sqs",
        region_name=region_name,
        config=Config(signature_version=signature_version),
    )

    # Attempt to read the maximum number of messages from the queue
    try:
        # SQS receive message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
        print("attempting to receive message from sqs")
        sqs_msg_response = sqs_client.receive_message(
            QueueUrl=sqs_restore,
            MaxNumberOfMessages=sqs_max_messages,
            WaitTimeSeconds=sqs_wait_time,
        )
        print("sqs message received")
        print(sqs_msg_response)
    except ClientError as e:
        # Trap failure to receive messages from SQS service
        print(f"Failed to receive message: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"error: failed to receive message - {e}")
        }

    # Process messages received
    if 'Messages' in sqs_msg_response:
        for msg in sqs_msg_response['Messages']:
            # Extract job parameters from the message body 
            print("msg")
            print(msg)
            data = json.loads(msg['Body'])['Message']
            print("data")
            print(data)
            sns_message = json.loads(data)
            print("sns_message")
            print(sns_message)
            archive_id = sns_message['ArchiveId']
            job_id = sns_message['JobId']
            print("archive_id")
            print(archive_id)
            print("job_id")
            print(job_id)
            # The unique receipt identifier is for deleting the message
            receipt_identifier = msg['ReceiptHandle']
    
            # Get job output (archive object)
            
            # Open a connection to Glacier
            glacier_client = boto3.client(
                "glacier",
                region_name=region_name,
                config=Config(signature_version=signature_version),
            )
            
            # Check glacier vault exist
            try:
                # List vaults: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/list_vaults.html
                print("attempting to list vaults")
                list_vaults_response = glacier_client.list_vaults()
                print("Listed vault")
                print(list_vaults_response)
                vault_exists = False
                for vault in list_vaults_response['VaultList']:
                    if vault['VaultName'] == glacier_vault_name:
                        vault_exists = True
                        print(f"Vault {glacier_vault_name} found")
                        break
                if not vault_exists:
                    raise Exception(f"Vault {glacier_vault_name} does not exist")
            except ClientError as e:
                # Trap failed to list vault
                print({
                    "code": 500,
                    "status": "error",
                    "message": f"error: failed to list glacier vault - {e}"
                }, 500)
                continue
            except Exception as e:
                # Trap glacier vault does not exist
                print({
                    "code": 404,
                    "status": "error",
                    "message": f"error: glacier vault does not exist - {e}"
                }, 404)
                continue
                
            # Get Archive object
            try:
                # Get job output: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html#
                # Glacier code examples: https://docs.aws.amazon.com/code-library/latest/ug/python_3_glacier_code_examples.html
                print("Attempting to get archive file from Glacier")
                get_job_output_response = glacier_client.get_job_output(
                    vaultName=glacier_vault_name,
                    jobId=job_id,
                )
                print("got archive file from Glacier")
                archive_object = get_job_output_response['body'].read()
                print("Read in object file")
                print(get_job_output_response)
            except ClientError as e:
                # Trap failure to get archive from Glacier
                print({
                    "code": 500,
                    "status": "error",
                    "message": f"error: failure to get archive from Glacier - {e}"
                }, 500)
                continue
            
            # Put the archive object into s3 gas-results bucket
            
            # Open a connection to s3
            s3_client = boto3.client(
                "s3",
                region_name=region_name,
                config=Config(signature_version=signature_version),
            )
            
            # Open a connection to dynamoDB
            db_client = boto3.client(
                "dynamodb",
                region_name=region_name,
                config=Config(signature_version=signature_version),
            )
            
            try:
                # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
                print("attempting to check if table exists")
                check_table_response = db_client.describe_table(TableName=table_name)
                print("checked dynamodb table exist")
                print(check_table_response)
            except ClientError as e:
                # Trap error to check table exists
                print({
                    "code": 404,
                    "status": "error",
                    "message": f"error: dynamodb table does not exist - {e}"
                }, 404)
                continue
            
            try:
                # Get the row of DynamoDB data based on the archiveId
                # Scan table: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/scan.html
                # Expression operators: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
                print("attempting to scan table for the row data based on archive id")
                scan_table_response = db_client.scan(
                    TableName=table_name,
                    FilterExpression='results_file_archive_id =:archive_id',
                    ExpressionAttributeValues={
                        ':archive_id': {'S': archive_id}
                    }
                )
                print("Got data for the job that was retrieved")
                print(scan_table_response)
                if scan_table_response['Items']:
                    archived_job_info = scan_table_response['Items'][0]
                    print(archived_job_info)
                else:
                    print({
                        "code": 404,
                        "status": "error",
                        "message": f"error: failed to find job info on DynamoDB based on archiveId"
                    }, 404)
                    continue
            except ClientError as e:
                # Trap error to scan table for jobs that belong to the user and archived
                print({
                    "code": 500,
                    "status": "error",
                    "message": f"error: failed to scan table for jobs that belong to the user and archived - {e}"
                }, 500)
                continue
            
            # Place object into s3 results bucket
            try:
                # put_object reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
                print("attempting to put object into bucket")
                key = f"{user_name}/"
                s3_client.put_object(
                    Bucket=s3_results_bucket,
                    Key=archived_job_info['s3_key_result_file']['S'],
                    Body=archive_object,
                )
                print("Success: object placed in results bucket")
            except ClientError as e:
                # Trap failed to put object into bucket error 
                print({
                    "code": 500,
                    "status": "error",
                    "message": f"error: failed to put object into bucket - {e}"
                }, 500)
                continue
            
            # Delete archive in Glacier
            try:
                print("attempting to delete archive")
                # delete_archive from Glacier: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
                archive_delete_response = glacier_client.delete_archive(
                    archiveId=archive_id,
                    vaultName=glacier_vault_name,
                )
                print("archive deleted")
                print(archive_delete_response)
            except ClientError as e:
                print({
                    "code": 500,
                    "status": "error",
                    "message": f"error: failed to delele archive from Glacier - {e}"
                }, 500)
                continue
            
            # Update dynamo to remove archive_id and retrieval info (EXPEDITED or STANDARD) since the archival is done
            try:
                print("attempting to remove archive id and retrieval info from DynamoDB")
                # Update table: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                db_update_response = db_client.update_item(
                    TableName=table_name,
                    Key={"job_id": {'S': archived_job_info['job_id']['S']}}, 
                    ExpressionAttributeValues={
                        ':archive_id': {'S': ""},
                        ':retrieval_status': {'S': ""}
                    },
                    UpdateExpression="SET results_file_archive_id = :archive_id, retrieval = :retrieval_status",
                )
                print("dynamoDB updated")
                print(db_update_response)
            except ClientError as e:
                print({
                    "code": 500,
                    "status": "error",
                    "message": f"error: failed to remove archive id and retrieval info from DynamoDB - {e}"
                }, 500)
                continue
            
            # Delete the message from the queue, if job was successfully submitted
            try:
                print("attempting to delete sqs message")
                # SQS delete message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
                sqs_delete_msg_response = sqs_client.delete_message(
                    QueueUrl=sqs_restore,
                    ReceiptHandle=receipt_identifier
                )
                print("message deleted from sqs")
            except ClientError as e:
                # Trap failure to delete messages from SQS service
                print({
                    "code": 500,
                    "status": "error",
                    "message": f"Failed to delete message: {e}"
                }, 500)
                continue
    
    
    print('Restoration request processed!')
    return {
        'statusCode': 200,
        'body': json.dumps('Restoration request processed!')
    }


### EOF
