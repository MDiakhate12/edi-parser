from boto3 import resource
from boto3 import client
import logging
import json
import os

from modules.main_layer import MainLayer

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
class CustomAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['simu_id'], msg), kwargs

def log_error(e: Exception, err_msg: str) -> None:
    logger.error(err_msg)
    logger.error(e, exc_info=True)
   
def lambda_handler(event, context):
    global logger

    logger_config = logging.getLogger(__name__)
    logger_config.setLevel(logging.INFO)
    logger = CustomAdapter(logger_config, {'simu_id': event["simulation_id"]})
    global s3_resource
    s3_resource = resource('s3')
    bucket_data_name = os.environ['S3_BUCKET_NAME']
    table_dynamoDB = os.environ['DynamoDB_TABLE_NAME']
    # bucket_data_name = "test-container-1234"
    # table_dynamoDB = "test-container-1234"

    
    reusePreviousResults = False
    try:
        main_layer = MainLayer(logger, event, reusePreviousResults, bucket_data_name, table_dynamoDB)
        main_layer.run_main()
        logger.info("Pre processing OK")
        
        status_code = 200
    
    except Exception as e:
        err_msg = f"There was an error while running the lambda handler..."
        log_error(e, err_msg)

        status_code = 400

    return {
        "statusCode": status_code
    }

def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    file = open("./event_local.json") 
    event = json.load(file)
    reusePreviousResults = event["reusePreviousResults"]

    try:
        main_layer = MainLayer(logger, event, reusePreviousResults)
        main_layer.run_main()
    
    except Exception as e:
        logger.error(f"There was an error while pre-processing the data...")
        logger.error(e, exc_info=True)

if __name__ == "__main__":
    main()
