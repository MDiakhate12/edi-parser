import logging
from boto3 import client
from botocore.exceptions import NoCredentialsError


def log_to_s3(logger:logging.Logger, bucket_name:str, key:str, log_content:str):
    """
    Uploads log content to an S3 bucket.

    Parameters:
    logger: logger defined in main scope.
    bucket_name (str): The name of the S3 bucket.
    key (str): The object key (path) within the bucket.
    log_content (str): The content of the log to be uploaded.

    Raises:
    NoCredentialsError: If AWS credentials are not found.
    """
    s3_client = client('s3')
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=log_content)

    except NoCredentialsError:
        logger.error("No AWS credentials found. Cannot upload log to S3.")


def write_records_dynamoDB(logger:logging.Logger, bucket_data_name: str, SimulationId: str, table_dynamoDB: str, target_key : str="", status : str="PENDING") -> None:
    """
    Write records to a DynamoDB table with details from the simulation.

    This function constructs a record based on the provided parameters and writes it to a DynamoDB table.
    Depending on the status, it generates a different path for the 'Document' attribute.

    Parameters:
    - logger (logging.Logger): Logger instance to log messages.
    - bucket_data_name (str): Name of the S3 bucket containing the data.
    - SimulationId (str): ID of the current simulation.
    - table_dynamoDB (str): Name of the DynamoDB table where the record should be written.
    - target_key (str, optional): S3 object key (path) in the S3 bucket. Defaults to an empty string.
    - status (str, optional): The status of the simulation. Can be "PENDING", "DONE", or other custom statuses.
                              Defaults to "PENDING".

    Returns:
    - None

    Raises:
    - ClientError: If there's an issue inserting the item in DynamoDB.

    Example Usage:
    >>> write_records_dynamoDB(logger, "sample-bucket", "12345", "sample-table", target_key="Bayplan.edi", status="DONE")
    # This will write a record to "sample-table" in DynamoDB with the given parameters and log the operation.
    """
    dynamodb = client('dynamodb')
    dataBucketPath = "s3://" + bucket_data_name + "/" + SimulationId + "/out/" + target_key if status != "PENDING" else "s3://" + bucket_data_name + "/" + SimulationId + "/intermediate/"

    dynamodb.put_item(TableName=table_dynamoDB, Item={
        'SimulationId': {'S': SimulationId},
        'Document': {'S': dataBucketPath},
        'status': {'S': status}
    })
    logger.info("insert item on dynamoDB")
     