# archive_app_config.py
# Set app configuration options for archive utility

import os

# Get the IAM username that was stashed at launch time
try:
    with open("/home/ubuntu/.launch_user", "r") as file:
        iam_username = file.read().replace("\n", "")
except FileNotFoundError as e:
    if "LAUNCH_USER" in os.environ:
        iam_username = os.environ["LAUNCH_USER"]
    else:
        # Unable to set username, so exit
        print("Unable to find launch user name in local file or environment!")
        raise e


class Config(object):

    CSRF_ENABLED = True

    AWS_REGION_NAME = "us-east-1"

    # AWS DynamoDB table
    AWS_DYNAMODB_ANNOTATIONS_TABLE = f"{iam_username}_annotations"

    # Signature version
    AWS_SIGNATURE_VERSION = "s3v4"

    # AWS S3 upload parameters
    AWS_S3_INPUTS_BUCKET = "gas-inputs"
    AWS_S3_RESULTS_BUCKET = "gas-results"

    HW = "16"

    # Accounts database
    ACCOUNTS_DATABASE = f"{iam_username}_accounts"

    # AWS SNS topics
    AWS_SNS_JOB_ARCHIVE_TOPIC = (
        f"arn:aws:sns:us-east-1:127134666975:{iam_username}_a{HW}_job_archive"
    )

    # AWS SQS queues
    AWS_SQS_WAIT_TIME = 20
    AWS_SQS_MAX_MESSAGES = 10
    AWS_SQS_ARCHIVE_QUEUE_NAME = (
        f"https://sqs.us-east-1.amazonaws.com/127134666975/{iam_username}_a{HW}_job_archive"
    )

    # AWS State Machine
    AWS_STATE_MACHINE = f"arn:aws:states:us-east-1:127134666975:stateMachine:{iam_username}_a{HW}_archive"

    # AWS Glacier vault
    AWS_GLACIER_VAULT_NAME = "ucmpcs"

### EOF
