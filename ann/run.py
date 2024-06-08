# run.py
# Runs the AnnTools pipeline
# NOTE: This file lives on the AnnTools instance and
# replaces the default AnnTools run.py

import sys
import time
import driver
import os
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")

BASE_DIR = os.path.abspath(os.path.dirname(__file__)) + "/"
JOBS_DIR = BASE_DIR + "jobs"
DATA_DIR = BASE_DIR + "data"

"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


def main():

    # Get job parameters
    jid = sys.argv[1]
    in_file = sys.argv[2]
    user_id = sys.argv[3]
    run_file = f"{JOBS_DIR}/{jid}~{in_file}"

    # Run the AnnTools pipeline
    with Timer():
        driver.run(run_file, "vcf")
        # Open a connection to s3
        s3_client = boto3.client(
            "s3",
            region_name=config["aws"]["AwsRegionName"],
            config=Config(signature_version=config["aws"]["AwsSignatureVersion"]),
        )
        s3_results_bucket = config["s3"]["ResultsBucketName"]
        cnet = config["DEFAULT"]["CnetId"]
        completed_jobs_id = set() # Used set to prevent duplicate jobs update just in case
        # Get all files in directory: https://www.geeksforgeeks.org/python-os-listdir-method/
        for file_name in os.listdir(JOBS_DIR):
            file_path = os.path.join(JOBS_DIR, file_name)
            key = f"{cnet}/{user_id}/{file_name}"
            job_id, f = file_name.split('~')
            fn_without_ext = f.split('.')[0]
            if file_name.endswith('.annot.vcf'):
                try:
                    # Upload files to AWS: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
                    response = s3_client.upload_file(file_path, s3_results_bucket, key)
                except ClientError as e:
                    print(f"error: failure to upload results file {file_name} - {e}")
                else:
                    # Add to completed job only if the file has been successfully uploaded
                    completed_jobs_id.add((job_id, fn_without_ext))
            elif file_name.endswith('.count.log'):
                try:
                    response = s3_client.upload_file(file_path, s3_results_bucket, key)
                except ClientError as e:
                    print(f"error: failure to upload log file {file_name} - {e}")
                else:
                    completed_jobs_id.add((job_id, fn_without_ext))

            if os.path.exists(file_path):
                # If file exists, delete file in instance
                try:
                    os.remove(file_path)
                except OSError as e:
                    print(f"error: failed to remove local job files: {e}")

        # Update jobs in DynamoDB
        db_client = boto3.client(
            "dynamodb",
            region_name=config["aws"]["AwsRegionName"],
            config=Config(signature_version=config["aws"]["AwsSignatureVersion"]),
        )
        # Check if table exists first
        table_name = config["gas"]["AnnotationsTable"]
        try:
            # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
            response = db_client.describe_table(TableName=table_name)
        except ClientError as e:
            print("error: table not found - {e}")
        # Update job info for each job
        job_completion_times = {}
        for job_id, fn_without_ext in completed_jobs_id:
            try:
                s3_key_result_file = f"{cnet}/{user_id}/{job_id}~{fn_without_ext}.annot.vcf"
                s3_key_log_file = f"{cnet}/{user_id}/{job_id}~{fn_without_ext}.vcf.count.log"
                complete_time = int(time.time())
                job_completion_times[job_id] = complete_time
                # Update table: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
                # Example of updating table on conditional: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
                response = db_client.update_item(
                    TableName=table_name,
                    Key={'job_id': {'S': job_id}},                        
                    ExpressionAttributeValues={
                        ':job_status': {'S': 'COMPLETED'},
                        ':s3_results_bucket': {'S': s3_results_bucket},
                        ':s3_key_result_file': {'S': s3_key_result_file},
                        ':s3_key_log_file': {'S': s3_key_log_file},
                        ':complete_time': {'N': str(complete_time)},
                    },
                    UpdateExpression=
                        'SET job_status = :job_status, \
                        s3_results_bucket = if_not_exists(s3_results_bucket, :s3_results_bucket), \
                        s3_key_result_file = if_not_exists(s3_key_result_file, :s3_key_result_file), \
                        s3_key_log_file = if_not_exists(s3_key_log_file, :s3_key_log_file), \
                        complete_time = if_not_exists(complete_time, :complete_time)', 
                    ReturnValues="ALL_NEW"
                )
            except ClientError as e:
                # Trap failure to update job info to DynamoDB
                print(f"error: failed to update job info in database - {e}")

        # Open a connection to sns
        sns_client = boto3.client(
            "sns",
            region_name=config["aws"]["AwsRegionName"],
            config=Config(signature_version=config["aws"]["AwsSignatureVersion"]),
        )
        sns_results_target = config["sns"]["ResultsTopic"]
        sns_archive_target = config["sns"]["ArchiveTopic"]
        for job_id, _ in completed_jobs_id:
            # Pass data needed for notification email
            notify_data = {
                "job_id": {"S": job_id}, 
                "complete_time": {"N": str(job_completion_times[job_id])},
                "user_id": {"S": user_id}, 
            }
            try:
                # Publish message to sns: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
                response = sns_client.publish(
                                # Use TargetArn for a specific target, TopicArn for multiple subscribers
                                TargetArn=sns_results_target,
                                Message=str(notify_data), 
                            )
            except ParamValidationError as e:
                # Trap parameter validation error
                print(f"Invalid parameter error: {e}")
            except ClientError as e:
                # Trap failure to publish notification to SNS service
                print(f"Failed to publish message: {e}")

            # Pass data needed for archival
            archive_data = {
                "job_id": {"S": job_id}, 
                "user_id": {"S": user_id}, 
                "results_key": {"S": s3_key_result_file},
            }
            try:
                # Publish message to sns: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
                response = sns_client.publish(
                                # Use TargetArn for a specific target, TopicArn for multiple subscribers
                                TargetArn=sns_archive_target,
                                Message=str(archive_data), 
                            )
            except ParamValidationError as e:
                # Trap parameter validation error
                print(f"Invalid parameter error: {e}")
            except ClientError as e:
                # Trap failure to publish notification to SNS service
                print(f"Failed to publish message: {e}")


if __name__ == "__main__":
    main()

### EOF
