# notify.py
# Notify user of job completion via email

import boto3
import json
import os
import sys
import time
from datetime import datetime

from botocore.exceptions import ClientError
from botocore.client import Config

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("notify_config.ini")

"""A12
Reads result messages from SQS and sends notification emails.
"""


def handle_results_queue(sqs=None):
    # Open a connection to sqs
    sqs_client = boto3.client(
        "sqs",
        region_name=config["aws"]["AwsRegionName"],
        config=Config(signature_version=config["aws"]["AwsSignatureVersion"]),
    )

    # Attempt to read the maximum number of messages from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    try:
        # SQS receive message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
        sqs_msg_response = sqs_client.receive_message(
            QueueUrl=sqs,
            MaxNumberOfMessages=int(config["sqs"]["MaxMessages"]),
            WaitTimeSeconds=int(config["sqs"]["WaitTime"]),
        )
    except ClientError as e:
        # Trap failure to receive messages from SQS service
        print({
            "code": 500,
            "status": "error",
            "message": f"Failed to receive message: {e}"
        }, 500)
        return 

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
                get_complete_time = data['complete_time']['N']                
                user_id = data['user_id']['S']
                if not job_id or not get_complete_time or not user_id:
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
                    "message": "error: key error"
                }, 400)
                continue

            # Get recipient
            try:
                profile = helpers.get_user_profile(id=user_id)
                user_email = profile["email"]
            except ClientError as e:
                print(f"Failed to get email/user's profile: {e}")
                continue
            except Exception as e:
                print(f"Failed to get email/user's profile: {e}")
                continue

            # Send email to user to notify them their job is complete
            try:
                # Format to human readable date and time
                timestamp = int(get_complete_time)
                submit_time_local = datetime.fromtimestamp(timestamp)
                complete_time = submit_time_local.strftime('%Y-%m-%d @ %H:%M:%S')
                # Get sender 
                sender = config["gas"]["MailDefaultSender"]
                # Get link to detailed job page
                detailed_link_page = f"{config['gas']['DetailedPageLink']}{job_id}"
                # Configure subject and message body
                subject = f"Results available for job {job_id}"
                body = f"Your annotation job completed at {complete_time}. Click here to view job details and results: {detailed_link_page}."
                # Send email
                response = helpers.send_email_ses(recipients=user_email, sender=sender, subject=subject, body=body)
            except Exception as e:
                # Trap failure to send email
                print({
                  "code": 500,
                  "status": "error",
                  "message": f"error: failure to launch the send email - {e}"
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


def main():

    # Get handles to SQS
    sqs = config["sqs"]["ResultsQueueName"]

    # Poll queue for new results and process them
    while True:
        handle_results_queue(sqs=sqs)


if __name__ == "__main__":
    main()

### EOF
