# views.py
# Application logic 

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError, ParamValidationError

from flask import abort, flash, redirect, render_template, request, session, url_for

from app import app, db
from decorators import authenticated, is_premium

from auth import get_profile

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.
"""


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():
    # Get bucket name, key, and job ID from the S3 redirect URL
    # Parsing url args: https://stackoverflow.com/questions/34671217/in-flask-what-is-request-args-and-how-is-it-used
    in_bucket = request.args.get('bucket')
    in_key = request.args.get('key')
    # Check if bucket and key exist in url redirected
    if not in_bucket or not in_key:
        # Trap bucket/key missing error
        app.logger.error("error: bucket/key missing")
        return abort(400)
    # Get the full filename, "UUID~input_file.ext"
    filename = in_key.split('/')[-1]
    # Get the job ID and input file name 
    job_ID, input_file = filename.split('~')

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
        s3.head_object(Bucket=in_bucket, Key=in_key)
    except ClientError as e:
        # Trap file not found error 
        app.logger.error(f"error: key/file not found - {e}")
        return abort(404)

    # Current time (seconds since Epoch): https://docs.python.org/3/library/time.html#time.time
    submit_time = int(time.time())

    user_id = session["primary_identity"]

    # Create a job item and persist it to the annotations database
    data = {
        "job_id": {"S": job_ID}, 
        "user_id": {"S": user_id}, 
        "input_file_name": {"S": input_file}, 
        "s3_inputs_bucket": {"S": in_bucket}, 
        "s3_key_input_file": {"S": in_key}, 
        "submit_time": {"N": str(submit_time)},
        "job_status": {"S": "PENDING"}
    }
    
    # Open a connection to dynamoDB
    db_client = boto3.client(
        "dynamodb",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )
    table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    try:
        # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
        response = db_client.describe_table(TableName=table_name)
    except ClientError as e:
        app.logger.error(f"error: table not found - {e}")
        return abort(404)
    try:
        # Adding a new item to DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/put_item.html
        # Additional reference: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
        response = db_client.put_item(
            TableName=table_name,
            Item=data
        )
    except ClientError as e:
        # Trap failure to add item to DynamoDB
        # Exception types: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/put_item.html
        app.logger.error(f"error: failed to persist job item to database - {e}")
        return abort(500)

    # Open a connection to sns
    sns_client = boto3.client(
        "sns",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )
    sns_target = app.config["AWS_SNS_JOB_REQUEST_TOPIC"]
    try:
        # Publish message to sns: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
        response = sns_client.publish(
                        # Use TargetArn for a specific target, TopicArn for multiple subscribers
                        TargetArn=sns_target,
                        Message=str(data),
                    )
    except ParamValidationError as e:
        # Trap parameter validation error
        app.logger.error(f"Invalid parameter error: {e}")
        return abort(400)
    except ClientError as e:
        # Trap failure to publish notification to SNS service
        app.logger.error(f"Failed to publish message: {e}")
        return abort(500)

    return render_template("annotate_confirm.html", job_id=job_ID)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():

    # Get list of annotations to display
    
    # Open a connection to dynamoDB
    db_client = boto3.client(
        "dynamodb",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )
    
    table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    user_id = session["primary_identity"]
    
    try:
        # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
        check_table_response = db_client.describe_table(TableName=table_name)
    except ClientError as e:
        app.logger.error(f"error: table not found - {e}")
        return abort(404)

    try:
        # Query an item in DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        # Examples: https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
        query_response = db_client.query(
            TableName=table_name,
            IndexName="user_id_index",
            ProjectionExpression="job_id, submit_time, input_file_name, job_status",
            KeyConditionExpression="user_id = :user_id",
            ExpressionAttributeValues={
                ':user_id': {'S': user_id}
            }
        )
    except ClientError as e:
        # Trap failure to query item from DynamoDB
        # Exception types: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        app.logger.error(f"error: failed to query database - {e}")
        return abort(500)

    # Extract the list of annotations
    annotations = query_response['Items']
    
    # Update time format
    for annotation in annotations:
        annotation['submit_time']['N'] = datetime_conversion(annotation['submit_time']['N'])

    return render_template("annotations.html", annotations=annotations)


"""Display details of a specific annotation job
"""

@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):
    # Open a connection to dynamoDB
    db_client = boto3.client(
        "dynamodb",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )
    
    table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    user_id = session["primary_identity"]
    
    try:
        # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
        check_table_response = db_client.describe_table(TableName=table_name)
    except ClientError as e:
        app.logger.error(f"error: table not found - {e}")
        return abort(404)

    try:
        # Query an item in DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        # Examples: https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
        query_response = db_client.query(
            TableName=table_name,
            ProjectionExpression=" \
                job_id, \
                submit_time, \
                input_file_name, \
                job_status, \
                complete_time, \
                s3_key_log_file, \
                s3_key_input_file, \
                s3_key_result_file, \
                s3_inputs_bucket, \
                s3_results_bucket, \
                user_id",
            KeyConditionExpression="job_id = :job_id",
            ExpressionAttributeValues={
                ':job_id': {'S': id},
            }
        )
    except ClientError as e:
        # Trap failure to query item from DynamoDB
        # Exception types: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        app.logger.error(f"error: failed to query database - {e}")
        return abort(500)

    # Extract the list of annotations
    annotation = query_response['Items'][0]

    if annotation['user_id']['S'] != user_id:
        # Trap forbidden access
        app.logger.error(f"error: forbidden access, job {id} does not belong to the user {user_id}")
        return abort(403)
    
    # Update time format
    annotation['submit_time']['N'] = datetime_conversion(annotation['submit_time']['N'])
    if 'complete_time' in annotation:
        annotation['complete_time']['N'] = datetime_conversion(annotation['complete_time']['N'])

    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )

    inputs_bucket = annotation['s3_inputs_bucket']['S']
    input_file_key = annotation['s3_key_input_file']['S']
    try:
        # Generate presigned url to download s3 object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
        # Example of how to use generate_presigned_url: https://stackoverflow.com/questions/56802869/how-can-i-let-a-user-download-a-file-from-an-s3-bucket-when-clicking-on-a-button
        input_file_presigned_url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': inputs_bucket, 'Key': input_file_key},
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL to download input file: {e}")
        return abort(500)
    annotation['input_file_link'] = {'S': input_file_presigned_url}
    
    try:
        user_profile = get_profile(identity_id=user_id)
        user_type = user_profile.role
    except ClientError as e:
        app.logger.error(f"Failed to get role/user's profile: {e}")
        return abort(500)
    except Exception as e:
        app.logger.error(f"Failed to get role/user's profile: {e}")
        return abort(500)
    
    block_results = False
    archived = False
    retrieval_in_progress = False
    try:
        # Check if results_file_archive_id is in dynamoDB to check if the file has been archived and also if file is in retrieval progress
        # get_item: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
        get_item_response = db_client.get_item(TableName=table_name, Key={'job_id': {'S': annotation['job_id']['S']}})
        if 'Item' in get_item_response and 'results_file_archive_id' in get_item_response['Item'] and get_item_response['Item']['results_file_archive_id']:
            archived = True
        if 'Item' in get_item_response and 'retrieval' in get_item_response['Item'] and get_item_response['Item']['retrieval']:
            retrieval_type = get_item_response['Item']['retrieval']['S']
            print(f"retrieval_type {retrieval_type}")
            retrieval_in_progress = retrieval_type
    except ClientError as e:
        app.logger.error(f"job item does not exist in database: {e}")
        return abort(404)
    
    # Only display results file if this is a premium user or free user with job completed in less than 3 mins
    if (user_type == "free_user" and archived) or (user_type == "premium_user" and archived and retrieval_in_progress):
        block_results = True
    if not block_results:
        if 's3_key_result_file' in annotation:
            results_bucket = annotation['s3_results_bucket']['S']
            result_file_key = annotation['s3_key_result_file']['S']
            try:
                # Generate presigned url to download s3 object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
                # Example of how to use generate_presigned_url: https://stackoverflow.com/questions/56802869/how-can-i-let-a-user-download-a-file-from-an-s3-bucket-when-clicking-on-a-button
                result_file_presigned_url = s3.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={'Bucket': results_bucket, 'Key': result_file_key},
                    ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
                )
            except ClientError as e:
                app.logger.error(f"Unable to generate presigned URL to download result file: {e}")
                return abort(500)
            annotation['result_file_link'] = {'S': result_file_presigned_url}

    return render_template("annotation.html", annotation=annotation, block_results=block_results, retrieval=retrieval_in_progress)


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    # Open a connection to dynamoDB
    db_client = boto3.client(
        "dynamodb",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )
    
    table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    user_id = session["primary_identity"]
    
    try:
        # Check if table exists: https://stackoverflow.com/questions/42485616/how-to-check-if-dynamodb-table-exists
        check_table_response = db_client.describe_table(TableName=table_name)
    except ClientError as e:
        app.logger.error(f"error: table not found - {e}")
        return abort(404)

    try:
        # Query an item in DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        # Examples: https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
        query_response = db_client.query(
            TableName=table_name,
            ProjectionExpression=" \
                job_id, \
                s3_key_log_file, \
                s3_results_bucket, \
                user_id",
            KeyConditionExpression="job_id = :job_id",
            ExpressionAttributeValues={
                ':job_id': {'S': id},
            }
        )
    except ClientError as e:
        # Trap failure to query item from DynamoDB
        # Exception types: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        app.logger.error(f"error: failed to query database - {e}")
        return abort(500)

    # Extract the list of annotations
    annotation = query_response['Items'][0]

    if annotation['user_id']['S'] != user_id:
        # Trap forbidden access
        app.logger.error(f"error: forbidden access, job {id} does not belong to the user {user_id}")
        return abort(403)

    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
    )

    log = ""
    if 's3_key_log_file' in annotation:
        results_bucket = annotation['s3_results_bucket']['S']
        log_file_key = annotation['s3_key_log_file']['S']

        try:
            # Get object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
            object_response = s3.get_object(Bucket=results_bucket, Key=log_file_key)
        except ClientError as e:
            # Trap object not found error
            # Exception types: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
            app.logger.error(f"error: log file not found - {e}")
            return abort(404)

        # Reading to text in Python: https://stackoverflow.com/questions/491921/unicode-utf-8-reading-and-writing-to-files-in-python
        log = object_response['Body'].read().decode('utf-8')

    return render_template("view_log.html", log=log)


"""Subscription management handler
"""
import stripe
from stripe.error import StripeError
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
@authenticated
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info
        return render_template("subscribe.html")

    elif request.method == "POST":
        # Process the subscription request
        stripe_token = request.form.get('stripe_token')
        # Check if stripe_token exist before doing anything further
        if not stripe_token:
            # Trap stripe_token missing error
            app.logger.error("error: stripe token missing")
            return abort(400)
        
        # Create a customer on Stripe
        # Stripe API Create Customer: https://docs.stripe.com/api/customers/create
        stripe.api_key = app.config["STRIPE_SECRET_KEY"]
        try:
            customer = stripe.Customer.create(
                email=session['email'],
                name=session['name'],
                card=stripe_token
            )
        except StripeError as e:
            app.logger.error(f"error: failed to create Stripe customer - {e}")
            return abort(500)

        # Subscribe customer to pricing plan
        try:
            subscription = stripe.Subscription.create(
                customer=customer.id,
                items=[{'price': app.config["STRIPE_PRICE_ID"]}],
            )
        except StripeError as e:
            app.logger.error(f"error: failed to subscribe customer - {e}")
            return abort(500)

        # Update user role in accounts database
        try:
            update_profile(identity_id=session["primary_identity"], role="premium_user")
        except Exception as e:
            app.logger.error(f"error: failed to update customer role to premium user - {e}")
            return abort(500)

        # Update role in the session
        session['role'] = "premium_user"

        # Cancel any pending archivals
        # Open a connection to Step Functions
        sfs = boto3.client(
            "stepfunctions",
            region_name=app.config["AWS_REGION_NAME"],
            config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
        )

        # Check state machine exists before trying to stop it
        try:
            # Describe state machine: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/describe_state_machine.html
            check_state_machine_response = sfs.describe_state_machine(
                stateMachineArn=app.config["AWS_STATE_MACHINE"]
            )
        except Exception as e:
            # Trap failure to find state machine
            app.logger.error(f"error: state machine does not exist - {e}")
            return abort(404)

        # List all state machine executions
        try:
            # List step function execution: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/list_executions.html
            executions_list = sfs.list_executions(
                stateMachineArn=app.config["AWS_STATE_MACHINE"],
                statusFilter='RUNNING'
            )
        except Exception as e:
            # Trap failure to list state machine execution
            app.logger.error(f"error: failed to list state machine execution - {e}")
            return abort(500)

        # Cancel execution of state machine
        for execution in executions_list['executions']:
            try:
                # Stop step function execution: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/stop_execution.html
                stop_state_machine_exec_response = sfs.stop_execution(
                    executionArn=execution['executionArn']
                )
            except ClientError as e:
                # Trap failure to stop step function execution
                app.logger.error(f"error: failed to stop step function execution - {e}")
                return abort(500)

        # Request restoration of the user's data from Glacier
        # Open a connection to sns
        sns = boto3.client(
            "sns",
            region_name=app.config["AWS_REGION_NAME"],
            config=Config(signature_version=app.config["AWS_SIGNATURE_VERSION"]),
        )
        sns_target = app.config["AWS_SNS_JOB_THAW_TOPIC"]
        try:
            user_id = session["primary_identity"]
            data = {
                "user_id": {"S": user_id}, 
            }
            # Publish message to sns: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
            sns_response = sns.publish(
                            # Use TargetArn for a specific target, TopicArn for multiple subscribers
                            TargetArn=sns_target,
                            Message=str(data),
                        )
        except ParamValidationError as e:
            # Trap parameter validation error
            app.logger.error(f"Invalid parameter error: {e}")
            return abort(400)
        except ClientError as e:
            # Trap failure to publish notification to SNS service
            app.logger.error(f"Failed to publish message: {e}")
            return abort(500)

        # Display confirmation page
        return render_template("subscribe_confirm.html", stripe_id=customer.id)

def datetime_conversion(input_time):
    # Get the current timestamp and convert to datetime object formatted: https://docs.python.org/3/library/datetime.html
    timestamp = int(input_time)
    submit_time_local = datetime.fromtimestamp(timestamp)
    submit_time_formatted = submit_time_local.strftime('%Y-%m-%d @ %H:%M:%S')
    return submit_time_formatted


"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact admin.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed;",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
