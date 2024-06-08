# thaw_app.py
# Thaws upgraded (Premium) user data

import boto3
import json
import requests
import sys
import time

from botocore.exceptions import ClientError
from botocore.client import Config
from flask import Flask, request, jsonify
sys.path.append('/home/ubuntu/gas/util')
from helpers import get_user_profile 

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration and add to Flask app object
environment = "thaw_app_config.Config"
app.config.from_object(environment)


@app.route("/", methods=["GET"])
def home():
    return f"This is the Thaw utility: POST requests to /thaw."


@app.route("/thaw", methods=["POST"])
def thaw_premium_user_data():
    
    # Check message type
    # https://docs.aws.amazon.com/sns/latest/dg/SendMessageToHttp.prepare.html
    message_type = request.headers.get('x-amz-sns-message-type')
    if message_type is None:
        print("Failure to get message type in request header")
        return (
            jsonify(
                {
                    "code": 400,
                    "status": "error",
                    "message": "Failure to get message type in request header"
                }
            ),
            400,
        )

    # Confirm SNS topic subscription
    if message_type == 'SubscriptionConfirmation':
        try:
            data = json.loads(request.data.decode('utf-8'))
            # Parsing data in sns message: https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html#http-header
            subscribe_url = data.get('SubscribeURL')
            response = requests.get(subscribe_url)
            print("Subscription confirmed.")
            return (
                jsonify(
                    {
                        "code": 201,
                        "message": "Subscription confirmed."
                    }
                ),
                201,
            )
        except Exception as e:
            print(f"Failure to process subscription confirmation {e}")
            return (
                jsonify(
                    {
                        "code": 400,
                        "status": "error",
                        "message": f"Failure to process subscription confirmation {e}"
                    }
                ),
                400,
            )
    elif message_type == 'Notification':
        # Process job request
        sqs = app.config["AWS_SQS_THAW_QUEUE_NAME"]

        # Open a connection to sqs
        sqs_client = boto3.client(
            "sqs",
            region_name=app.config["AWS_REGION_NAME"],
            config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
        )

        # Attempt to read the maximum number of messages from the queue
        try:
            # SQS receive message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
            sqs_msg_response = sqs_client.receive_message(
                QueueUrl=sqs,
                MaxNumberOfMessages=int(app.config["AWS_SQS_MAX_MESSAGES"]),
                WaitTimeSeconds=int(app.config["AWS_SQS_WAIT_TIME"]),
            )
            print("sqs message received")
        except ClientError as e:
            # Trap failure to receive messages from SQS service
            print(f"Failed to receive message: {e}")
            return (
                jsonify(
                    {
                        "code": 500,
                        "status": "error",
                        "message": f"Failed to receive message: {e}"
                    }
                ),
                500,
            )

        # Process messages received
        if 'Messages' in sqs_msg_response:
            for msg in sqs_msg_response['Messages']:
                # Extract job parameters from the message body 
                data = json.loads(msg['Body'])['Message']
                data = data.replace("'", '"')
                data = json.loads(data)
                print("data")
                print(data)
                # The unique receipt identifier is for deleting the message
                receipt_identifier = msg['ReceiptHandle']

                try:
                    user_id = data['user_id']['S']
                    if not user_id:
                        print({
                            "code": 400,
                            "status": "error",
                            "message": "error: empty field value"
                        }, 400)
                        continue
                except KeyError as e:
                    print({
                        "code": 400,
                        "status": "error",
                        "message": f"error: key error {e}"
                    }, 400)
                    continue

                print("data loaded from message")
                # Check if the user is a premium user before doing any data retrieval task             
                try:
                    profile = get_user_profile(id=user_id, db_name=app.config["ACCOUNTS_DATABASE"])
                    user_type = profile['role']
                    print(f"user profile/type checked - {user_type}")
                except ClientError as e:
                    print(f"Failed to get email/user's profile: {e}")
                    continue
                except Exception as e:
                    print(f"Failed to get email/user's profile: {e}")
                    continue   
                
                if user_type == "premium_user":

                    # Open a connection to dynamoDB
                    db_client = boto3.client(
                        "dynamodb",
                        region_name=app.config["AWS_REGION_NAME"],
                        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
                    )
                    
                    table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
                    
                    try:
                        # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
                        check_table_response = db_client.describe_table(TableName=table_name)
                        print("checked dynamodb table exist")
                    except ClientError as e:
                        # Trap error to check table exists
                        print({
                            "code": 404,
                            "status": "error",
                            "message": f"error: dynamodb table does not exist - {e}"
                        }, 404)
                        continue

                    try:
                        # Get all jobs which belong to the user and that the jobs are archived: https://dynobase.dev/dynamodb-python-with-boto3/#scan
                        # Scan table: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/scan.html
                        # Expression operators: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
                        param_filter = 'user_id =:user_id AND attribute_exists(results_file_archive_id) \
                            AND results_file_archive_id <> :empty'
                        param_expression = {
                            ':user_id': {'S': user_id},
                            ':empty': {'S': ""}
                        }
                        params = {
                            'TableName': table_name,
                            'FilterExpression': param_filter,
                            'ExpressionAttributeValues': param_expression
                        }
                        archived_jobs_list = []
                        while True:
                            scan_table_response = db_client.scan(**params)
                            print("scan_table_response")
                            print(scan_table_response)
                            table_rows_data = scan_table_response['Items']
                            archived_jobs_list.extend(table_rows_data)
                            if 'LastEvaluatedKey' in scan_table_response:
                                params['ExclusiveStartKey'] = scan_table_response['LastEvaluatedKey']
                            else:
                                break
                        archive_identifiers = []
                        for archived_job in archived_jobs_list:
                            archive_identifiers.append((archived_job['results_file_archive_id']['S'], archived_job['job_id']['S']))
                            print(f"add archive id and job id: {archived_job['results_file_archive_id']['S']}")
                        print(archive_identifiers)
                        print("Got jobs that belong to the user and archived")
                    except ClientError as e:
                        # Trap error to scan table for jobs that belong to the user and archived
                        print({
                            "code": 500,
                            "status": "error",
                            "message": f"error: failed to scan table for jobs that belong to the user and archived- {e}"
                        }, 500)
                        continue

                    # Check glaicer vault exists
                    # Open a connection to Glacier
                    glacier_client = boto3.client(
                        "glacier",
                        region_name=app.config["AWS_REGION_NAME"],
                        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
                    )

                    vault_name = app.config["AWS_GLACIER_VAULT_NAME"]
                    
                    # Check glacier vault exist
                    try:
                        # List vaults: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/list_vaults.html
                        list_vaults_response = glacier_client.list_vaults()
                        print("Listed vault")
                        vault_exists = False
                        for vault in list_vaults_response['VaultList']:
                            if vault['VaultName'] == vault_name:
                                vault_exists = True
                                print(f"Vault {vault_name} found")
                                break
                        if not vault_exists:
                            raise Exception(f"Vault {vault_name} does not exist")
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

                    # Initialize retrieval (try expedited tier first, if failed then try standard)
                    sns_restore_topic = app.config["AWS_SNS_JOB_RESTORE_TOPIC"]
                    for archive_id, job_id in archive_identifiers:
                        print(f"archive id: {archive_id}")
                        print(f"job id: {job_id}")
                        expedited = False
                        standard = False
                        try:
                            print("Attempting to initiate retrieval with expedited tier")
                            job_params = {
                                'SNSTopic': sns_restore_topic,
                                'Type': 'archive-retrieval',
                                'ArchiveId': archive_id,
                                'Tier': 'Expedited',
                            }
                            # Initiate job explanation: https://docs.aws.amazon.com/amazonglacier/latest/dev/api-initiate-job-post.html
                            # Initiate job API reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
                            glacier_expedited_job_response = glacier_client.initiate_job(
                                vaultName=vault_name,
                                jobParameters=job_params,
                            )
                            expedited = True
                            print("Retrieval processed with expedited tier")
                        except ClientError as e:
                            # Error handling 1: https://docs.aws.amazon.com/amazonglacier/latest/dev/api-error-responses.html
                            # Error handling 2: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
                            if e.response['Error']['Code'] == 'InsufficientCapacityException':
                                # Trap failed in expedited job, proceed with standard job
                                print("expedited request failed")
                                print("Attempting to initiate retrieval with standard tier")
                                job_params['Tier'] = 'Standard'
                                try:
                                    glacier_standard_job_response = glacier_client.initiate_job(
                                        vaultName=vault_name, 
                                        jobParameters=job_params
                                    )
                                    standard = True
                                    print("Retrieval processed with standard tier")
                                except ClientError:
                                    # Raise the error to the outer except statement to handle
                                    print("standard request failed")
                                    raise
                            else:
                                # Trap error to initiate glacier retrieval job
                                print({
                                    "code": 500,
                                    "status": "error",
                                    "message": f"error: failed to initiate glacier retrieval job - {e}"
                                }, 500)
                                continue

                        # Based on the tier executed for the request, update the dynamoDB information
                        if expedited and not standard:
                            thaw_tier = 'EXPEDITED'
                        elif not expedited and standard:
                            thaw_tier = 'STANDARD'
                        try:
                            print("Attempting to update DynamoDB database with retrieval tier")
                            # Update table: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                            db_update_response = db_client.update_item(
                                TableName=table_name,
                                Key={"job_id": {'S': job_id}}, 
                                ExpressionAttributeValues={':thaw_tier': {'S': thaw_tier}},
                                UpdateExpression="SET retrieval = :thaw_tier",
                            )
                            print("Updated DynamoDB database with retrieval tier")
                        except ClientError as e:
                            # Trap failure to update retrieval tier to DynamoDB
                            print({
                                "code": 500,
                                "status": "error",
                                "message": f"error: failed to update dynamodb database with retrieval tier - {e}"
                            }, 500)
                            continue

                # Delete the message from the queue, if job was successfully submitted
                try:
                    # SQS delete message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
                    sqs_response = sqs_client.delete_message(
                        QueueUrl=sqs,
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

    print("Processed retrieval request")
    return (
        jsonify(
            {
                "code": 201,
                "message": "Processed retrieval request"
            }
        ),
        201,
    )


### EOF
