from boto3 import resource
from boto3 import client
from botocore.exceptions import NoCredentialsError
import logging
import json
import os
import sys
import traceback

sys.path.insert(0, "/var/task/Pre-processing-CICD-1")
from modules.main_layer import MainLayer
from modules.data_layer import DataLayer as DL


class CustomAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['simu_id'], msg), kwargs
    
def log_to_s3(simu_id:str, bucket_name:str, log_content:str):
    s3_client = client('s3')
    try:
        
        s3_client.put_object(Bucket=bucket_name, Key=f'{simu_id}/out/error.txt', Body=log_content)
        
    except NoCredentialsError:
        print("No AWS credentials found. Cannot upload log to S3.")
        
def log_error(e: Exception, err_msg: str, simu_id:str, event:dict, s3_bucket:str="") -> None:
    logger.error(err_msg)
    logger.error(e, exc_info=True)

    error_message = str(e)
    if "errorNo" not in error_message:
        context_details = {
            'event': event,  # Print the Lambda event for more context
            'traceback': traceback.format_exc()  # Capture the traceback for detailed error info
            
        }
        context_details['traceback'].replace(r'\n', '\n')
    else: 
        context_details = {
            'event': event,  # Print the Lambda event for more context
            'traceback': ""  # Capture the traceback for detailed error info
            
        }
        
    log_content = f"{err_msg} \n{str(e)} \n\n Lambda trigger event: {context_details['event']} \n\n{context_details['traceback']}"
    log_to_s3(simu_id, s3_bucket, log_content) 

def write_records_dynamoDB(bucket_data_name: str, SimulationId: str, table_dynamoDB: str, target_key : str="", status : str="PENDING") -> None:
        dynamodb = client('dynamodb')
        dataBucketPath = "s3://" + bucket_data_name + "/" + SimulationId + "/out/" + target_key if status != "PENDING" else "s3://" + bucket_data_name + "/" + SimulationId + "/intermediate/" 
        dynamodb.put_item(TableName=table_dynamoDB, Item={
        'SimulationId': {'S': SimulationId},
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
    table_dynamoDB = os.environ['DynamoDB_TABLE_NAME']
    bucket_data_name = bucket_output_name = os.environ['S3_BUCKET_NAME']
    
    try:
        bucket_ref_name = os.environ['s3_ref_bucket']
    except:
        bucket_ref_name = "optistow-referential-bucket"
    
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
            write_records_dynamoDB(bucket_data_name, simulation_id,  table_dynamoDB, target_key = "Bayplan.edi", status = 'DONE')
        status_code = 200
    
    except Exception as e:
        simulation_id = event["simulation_id"]
        if reusePreviousResults == False:
            write_records_dynamoDB(bucket_data_name, simulation_id, table_dynamoDB, target_key = "error.txt", status = 'PRE-KO')
            err_msg = "There was an error while running the lambda handler for Pre_Processing..."
            log_error(e, err_msg, simulation_id, event, bucket_data_name)
        else:
            write_records_dynamoDB(bucket_data_name, simulation_id, table_dynamoDB, target_key = "error.txt", status = 'POST-KO')
            err_msg = f"There was an error while running the lambda handler for Post-Processing..."
            log_error(e, err_msg, simulation_id, event, bucket_data_name)
            

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
