# archive_app.py
# Archive Free user data

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
environment = "archive_app_config.Config"
app.config.from_object(environment)


@app.route("/", methods=["GET"])
def home():
    return f"This is the Archive utility: POST requests to /archive."


@app.route("/archive", methods=["POST"])
def archive_free_user_data():

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
        sqs = app.config["AWS_SQS_ARCHIVE_QUEUE_NAME"]

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
                # The unique receipt identifier is for deleting the message
                receipt_identifier = msg['ReceiptHandle']

                try:
                    job_id = data['job_id']['S']
                    user_id = data['user_id']['S']
                    results_key = data['results_key']['S']
                    if not job_id or not user_id or not results_key:
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
                # Check if the user is a free user before doing any archival task             
                try:
                    profile = get_user_profile(id=user_id, db_name=app.config["ACCOUNTS_DATABASE"])
                    user_type = profile['role']
                    print("user profile/type checked")
                except ClientError as e:
                    print(f"Failed to get email/user's profile: {e}")
                    continue
                except Exception as e:
                    print(f"Failed to get email/user's profile: {e}")
                    continue   
                
                if user_type == "free_user":
                    print("free user")
                    # Open a connection to Step Functions
                    sfs = boto3.client(
                        "stepfunctions",
                        region_name=app.config["AWS_REGION_NAME"],
                        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
                    )

                    try:
                        # Describe state machine: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/describe_state_machine.html
                        check_state_machine_response = sfs.describe_state_machine(stateMachineArn=app.config["AWS_STATE_MACHINE"])
                        print("checked step function state machine exist")
                    except Exception as e:
                        print({
                            "code": 404,
                            "status": "error",
                            "message": f"error: state machine does not exist - {e}"
                        }, 404)
                        continue

                    # Start run procedure in the state machine
                    try:
                        # Data needed to pass into state machine 
                        data_to_step_func = {
                            'job_id': job_id,
                            'results_key': results_key,
                            's3_results_bucket': app.config["AWS_S3_RESULTS_BUCKET"],
                            'region_name': app.config["AWS_REGION_NAME"],
                            'signature_version': app.config["AWS_SIGNATURE_VERSION"],
                            'table_name' : app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"],
                            'glacier_vault_name': app.config["AWS_GLACIER_VAULT_NAME"],
                        }
                        # Request step function to start execution: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/start_execution.html
                        sfs_response = sfs.start_execution(
                            stateMachineArn=app.config["AWS_STATE_MACHINE"],
                            input=json.dumps(data_to_step_func)
                        )
                        print("step function started execution")
                    except ClientError as e:
                        # Trap failure to start step function execution
                        print({
                            "code": 500,
                            "status": "error",
                            "message": f"error: failure to start step function execution - {e}"
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

    print("Success: Archive request processed.")
    return (
        jsonify(
            {
                "code": 201,
                "message": "Success: Archive request processed."
            }
        ),
        201,
    )


### EOF
