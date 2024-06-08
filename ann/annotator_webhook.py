# annotator_webhook.py
#
# Modified to run as a web server that can be called by SNS to process jobs
# Run using: python annotator_webhook.py
# NOTE: This file lives on the AnnTools instance

import boto3
import requests
import os
import json
import sys
import time
from subprocess import Popen, PIPE
from flask import Flask, jsonify, request
from botocore.client import Config
from botocore.exceptions import ClientError

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration and add to Flask app object
environment = "annotator_webhook_config.Config"
app.config.from_object(environment)

# Connect to SQS and get the message queue


@app.route("/", methods=["GET"])
def annotator_webhook():

    return ("Annotator webhook; POST job to /process-job-request"), 200


"""
Replace polling with webhook in annotator

Receives request from SNS; queries job queue and processes message.
Reads request messages from SQS and runs AnnTools as a subprocess.
Updates the annotations database with the status of the request.
"""


@app.route("/process-job-request", methods=["POST"])
def annotate():

    # Check message type
    # https://docs.aws.amazon.com/sns/latest/dg/SendMessageToHttp.prepare.html
    message_type = request.headers.get('x-amz-sns-message-type')
    if message_type is None:
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
        sqs = app.config["AWS_SQS_REQUESTS_QUEUE_NAME"]

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
        except ClientError as e:
            # Trap failure to receive messages from SQS service
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
                # The unique receipt identifier is for deleting the message
                receipt_identifier = msg['ReceiptHandle']

                try:
                    job_id = data['job_id']['S']
                    user_id = data['user_id']['S']
                    input_file_name = data['input_file_name']['S']
                    s3_inputs_bucket = data['s3_inputs_bucket']['S']
                    s3_key_input_file = data['s3_key_input_file']['S']
                    submit_time = data['submit_time']['N']
                    job_status = data['job_status']['S']
                    if not job_id or not user_id or not input_file_name or not s3_inputs_bucket or not s3_key_input_file or not submit_time or not job_status:
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

                # Open a connection to s3
                s3 = boto3.client(
                    "s3",
                    region_name=app.config["AWS_REGION_NAME"],
                    config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
                )
                try:
                    # Check if key exists in instance: https://towardsthecloud.com/aws-sdk-key-exists-s3-bucket-boto3
                    # head_object reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html
                    # head_object retrieves metadata from an object without returning the object itself
                    # If key does not exist, it will throw error
                    s3.head_object(Bucket=s3_inputs_bucket, Key=s3_key_input_file)
                except ClientError as e:
                    # Trap key/file not found error 
                    print({
                        "code": 404,
                        "status": "error",
                        "message": f"error: key/file not found - {e}"
                    }, 404)
                    continue

                # Create directory for storing jobs info: https://www.geeksforgeeks.org/how-to-create-directory-if-it-does-not-exist-using-python/
                if not os.path.exists('jobs'):
                    try:
                        os.makedirs('jobs')
                    except IOError as e:
                        print({
                            "code": 500,
                            "status": "error",
                            "message": f"error: failed to set up working dir/files {str(e)}",
                        }, 500)
                        continue
                # Set path to dump the file to be downloaded
                filename = job_id + "~" + input_file_name
                downloaded_file_path = os.path.join("jobs", filename)

                # Get the input file S3 object and copy it to a local file
                try:
                    # Download file from s3: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
                    s3.download_file(s3_inputs_bucket, s3_key_input_file, downloaded_file_path)
                except ClientError as e:
                    # Trap failure to download error 
                    print({
                      "code": 500,
                      "status": "error",
                      "message": f"error: failed to download file - {e}"
                    }, 500)
                    continue

                # Launch annotation job as a background process
                try:
                    # Spawn a subprocess: https://docs.python.org/3/library/subprocess.html#popen-constructor  
                    Popen(['python', 'run.py', job_id, input_file_name, user_id]) 
                except Exception as e:
                    print({
                      "code": 500,
                      "status": "error",
                      "message": f"error: failure to launch the annotator job - {e}"
                    }, 500)
                    continue

                # Update job status to running
                job_status = "RUNNING"
                # Check if table exists first
                # Open a connection to dynamodb
                db_client = boto3.client(
                    "dynamodb",
                    region_name=app.config["AWS_REGION_NAME"],
                    config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
                )
                table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
                try:
                    # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
                    db_describe_response = db_client.describe_table(TableName=table_name)
                except ClientError as e:
                    print({
                        "code": 404,
                        "status": "error",
                        "message": f"error: table not found - {e}"
                    }, 404)
                    continue
                # Update job status only if the current job status is pending
                try:
                    # Update table: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                    # Example of updating table on conditional: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
                    db_update_response = db_client.update_item(
                        TableName=table_name,
                        Key={"job_id": {'S': job_id}}, 
                        ExpressionAttributeValues={
                            ':RUNNING': {'S': 'RUNNING'},
                            ':PENDING': {'S': 'PENDING'},
                        },
                        UpdateExpression="SET job_status = :RUNNING",
                        ConditionExpression="job_status = :PENDING",
                        ReturnValues="ALL_NEW"
                    )
                except ClientError as e:
                    # Trap failure to update job status to DynamoDB
                    print({
                        "code": 500,
                        "status": "error",
                        "message": f"error: failed to update job status in database - {e}"
                    }, 500)
                    continue

                # Delete the message from the queue, if job was successfully submitted
                try:
                    # SQS delete message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
                    sqs_response = sqs_client.delete_message(
                        QueueUrl=sqs,
                        ReceiptHandle=receipt_identifier
                    )
                except ClientError as e:
                    # Trap failure to delete messages from SQS service
                    print({
                        "code": 500,
                        "status": "error",
                        "message": f"Failed to delete message: {e}"
                    }, 500)
                    continue

    return (
        jsonify(
            {
                "code": 201,
                "message": "Annotation job request processed."
            }
        ),
        201,
    )


### EOF
