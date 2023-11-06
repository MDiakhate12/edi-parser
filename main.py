import logging
import json
import os
import sys
import traceback
from boto3 import resource

sys.path.insert(0, "/var/task/Pre-processing-CICD-1")

from modules.aws_utils import log_to_s3, write_records_dynamoDB
from modules.main_layer import MainLayer


class CustomAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['simu_id'], msg), kwargs 

def configure_logger(simulation_id: str):
    """
    Configure a logger with a custom adapter to prepend the simulation ID to log messages.
    
    This function initializes a logger with the given name and sets its level to INFO. 
    The logger is then wrapped in a CustomAdapter to prepend each log message with the 
    simulation ID provided as an argument. This helps in differentiating log entries 
    originating from different simulation runs.
    
    Parameters:
    - simulation_id (str): The ID of the simulation, which will be prepended to log messages.
    
    Returns:
    - CustomAdapter: A logger adapter that prepends the simulation ID to log messages.
    
    Example Usage:
    >>> logger = configure_logger("12345")
    >>> logger.info("This is a log message.")
    [12345] This is a log message.
    """
    logger_config = logging.getLogger(__name__)
    logger_config.setLevel(logging.INFO)
    return CustomAdapter(logger_config, {'simu_id': simulation_id})

def log_and_store_error(e: Exception, err_msg: str, simu_id:str, event:dict, s3_bucket:str="") -> None:
    """
    Log the error to the console and store the detailed error message in an S3 bucket.

    This function logs the provided error message and exception details to the console.
    It then formats and stores the detailed error message, the Lambda event that triggered the error,
    and the full traceback to a specified S3 bucket.

    Parameters:
    - e (Exception): The exception instance that was raised.
    - err_msg (str): A custom error message to be logged and stored.
    - simu_id (str): The simulation ID, used to create a unique log path in the S3 bucket.
    - event (dict): The Lambda event that triggered the function.
    - s3_bucket (str, optional): The name of the S3 bucket where the error details should be stored. Defaults to an empty string.

    Returns:
    None

    Notes:
    - If the exception message contains "errorNo", only the exception message is logged and stored, without the traceback.
    - The stored error details are saved in a file named 'error.txt' within a directory named after the simulation ID.
    """

    logger.error(err_msg)
    logger.error(e, exc_info=True)

    error_content = str(e) if "errorNo" not in str(e) else ""
    context_details = {
        'event': event,
        'traceback': error_content or traceback.format_exc()
    }

    log_content = (f"{err_msg} \n{str(e)} \n\n Lambda trigger event: {context_details['event']} "
                   f"\n\n{context_details['traceback']}")
    log_path = f'{simu_id}/out/error.txt'
    log_to_s3(logger, s3_bucket, log_path, log_content)

def handle_error(e: Exception, event: dict, bucket_data_name: str, table_dynamoDB: str, reuse_previous_results: bool):
    """
    Handle exceptions by logging the error, storing details in S3, and writing records to DynamoDB.

    This function is designed to provide a unified error handling mechanism. Depending on 
    the context (pre-processing or post-processing), it updates the DynamoDB with a status 
    indicating the type of error, logs the error, and stores detailed error information 
    in an S3 bucket. The nature of the error (pre-processing or post-processing) is determined 
    by the `reuse_previous_results` flag.

    Parameters:
    - e (Exception): The exception instance that was raised.
    - event (dict): The Lambda event that triggered the function.
    - bucket_data_name (str): The name of the S3 bucket where the error details should be stored.
    - reuse_previous_results (bool): A flag indicating whether the error occurred during 
      post-processing (`True`) or pre-processing (`False`).

    Returns:
    - int: A status code of 400, indicating that an error has occurred.

    Example Usage:
    >>> handle_error(Exception("Sample Error"), {"simulation_id": "12345"}, "sample-bucket", False)
    # This will log the error, store error details in "sample-bucket", and write records to DynamoDB 
    # indicating a pre-processing error.
    400
    """
    simulation_id = event["simulation_id"]
    target_key = "error.txt"

    if reuse_previous_results:
        write_records_dynamoDB(bucket_data_name, simulation_id, table_dynamoDB, target_key, status='POST-KO')
        err_msg = "There was an error while running the lambda handler for Post-Processing..."
    else:
        write_records_dynamoDB(bucket_data_name, simulation_id, table_dynamoDB, target_key, status='PRE-KO')
        err_msg = "There was an error while running the lambda handler for Pre_Processing..."
    
    log_and_store_error(e, err_msg, simulation_id, event, bucket_data_name)
    return 400

def lambda_handler(event, context):
    """
    AWS Lambda entry point that processes the given event.

    This handler initializes the logger, extracts relevant environment variables and 
    parameters from the Lambda event. It then processes the event using the `MainLayer` class.
    Depending on the outcome of the processing and the value of `reuse_previous_results`, it 
    writes records to DynamoDB. If an exception is encountered, error handling is delegated 
    to the `handle_error` function.

    Parameters:
    - event (dict): The Lambda event that triggered the function.
    - context (object): The runtime information provided by AWS Lambda.

    Returns:
    - dict: A dictionary containing the HTTP status code.
    """
    global logger, s3_resource, dynamodb
    s3_resource = resource('s3')
    dynamodb = resource('dynamodb')
    logger = configure_logger(event["simulation_id"])

    table_dynamoDB = os.environ['DynamoDB_TABLE_NAME']
    bucket_data_name = os.environ['S3_BUCKET_NAME']
    bucket_ref_name = os.environ.get('s3_ref_bucket', "optistow-referential-bucket")
    reuse_previous_results = event.get("reusePreviousResults", False)

    try:
        main_layer = MainLayer(logger, event, reuse_previous_results, bucket_data_name, bucket_ref_name)
        main_layer.run_main()
        logger.info("Pre processing OK")

        if not reuse_previous_results:
            write_records_dynamoDB(logger, bucket_data_name, event["simulation_id"], table_dynamoDB, status='PENDING')
        else:
            write_records_dynamoDB(logger, bucket_data_name, event["simulation_id"], table_dynamoDB, target_key="Bayplan.edi", status='DONE')
        return {"statusCode": 200}

    except Exception as e:
        status_code = handle_error(e, event, bucket_data_name, table_dynamoDB, reuse_previous_results)
        return {"statusCode": status_code}

def main():
    with open("./event_local.json", "r") as file:
        event = json.load(file)
    logger = configure_logger(event["simulation_id"])
    try:
        main_layer = MainLayer(logger, event, event.get("reusePreviousResults", False))
        main_layer.run_main()

    except Exception as e:
        logger.error("There was an error while pre-processing the data...")
        logger.error(e, exc_info=True)


if __name__ == "__main__":
    main()