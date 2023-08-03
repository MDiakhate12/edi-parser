from boto3 import resource
from boto3 import client
import logging
import json
import os
import sys
import datetime 

# sys.path.insert(0, "/var/task/Pre-processing-CICD-1")
from modules.main_layer import MainLayer
from modules.data_layer import DataLayer as DL

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
    

def write_records_dynamoDB(bucket_data_name: str, SimulationId: str, table_dynamoDB: str, target_key : str="", status : str="PENDING") -> None:
        dynamodb = client('dynamodb')
        dataBucketPath = "s3://" + bucket_data_name + "/" + SimulationId + "/out/" + target_key if status != "PENDING" else "s3://" + bucket_data_name + "/" + SimulationId + "/intermediate/" 
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dynamodb.put_item(TableName=table_dynamoDB, Item={
        'SimulationId': {'S': SimulationId},
        'Update_Time': {'S': current_time},  # Corrected syntax for datetime
        'Document': {'S': dataBucketPath},
        'status': {'S': status}
    })
        logger.info("insert item on dynamoDB")
   
def lambda_handler(event, context):
    global logger

    logger_config = logging.getLogger(__name__)
    logger_config.setLevel(logging.INFO)
    logger = CustomAdapter(logger_config, {'simu_id': event["simulation_id"]})
    global s3_resource, dynamodb 
    s3_resource = resource('s3')
    dynamodb = resource('dynamodb')
    table_dynamoDB = os.environ['dynamo_db_status']
    bucket_ref_name = os.environ['s3_ref_bucket']
    bucket_data_name = bucket_output_name = os.environ['s3_simulation_bucket']
    # table_dynamoDB = "simul-optistow-vessel"
    # bucket_ref_name = "optistow-preprocessing-test-ref"
    # bucket_data_name = bucket_output_name = "optistow-preprocessing-test-out"
    try:
        reusePreviousResults = event["reusePreviousResults"]
    except:
        reusePreviousResults = False
        
    
    try:
        main_layer = MainLayer(logger, event, reusePreviousResults,bucket_output_name, bucket_ref_name)
        main_layer.run_main()
        logger.info("Pre processing OK")
        simulation_id = event["simulation_id"]
        
        if reusePreviousResults == False:
            write_records_dynamoDB(bucket_data_name, simulation_id, table_dynamoDB, status = 'PENDING')
        else:
            write_records_dynamoDB(bucket_data_name, simulation_id, table_dynamoDB, target_key = "Bayplan.edi", status = 'DONE')
        status_code = 200
    
    except Exception as e:
        simulation_id = event["simulation_id"]
        write_records_dynamoDB(bucket_data_name, simulation_id, table_dynamoDB, target_key = "error.csv", status = 'ERROR')
        processing_phase = 'preprocessing' if reusePreviousResults == False else 'post_processing'
        err_msg = f"There was an error while running the lambda handler for {processing_phase}..."
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
    try:
        reusePreviousResults = event["reusePreviousResults"]
    except:
        reusePreviousResults = False
    
    try:
        main_layer = MainLayer(logger, event, reusePreviousResults)
        main_layer.run_main()

    except Exception as e:
        logger.error(f"There was an error while pre-processing the data...")
        logger.error(e, exc_info=True)

if __name__ == "__main__":
    main()
