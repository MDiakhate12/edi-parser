from boto3 import resource
from boto3 import client
import logging
import json
import os
import sys

from modules.main_layer import MainLayer

class CustomAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['simu_id'], msg), kwargs


# logging.basicConfig(stream=sys.stdout, level=logging.INFO)
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# logger.info("getting args")
# logger.info(sys.argv[1]) # prints var1
# logger.info(sys.argv[2]) # prints var2


bucket_data_name = sys.argv[1]
vesselImo = sys.argv[2]
voyage = sys.argv[3] 
port = sys.argv[4]
last_pod_seq_num = sys.argv[5] 
timestamp = sys.argv[6]
description = sys.argv[7]
path = sys.argv[8]
simulation_id = sys.argv[9]

def log_error(e: Exception, err_msg: str) -> None:
    logger.error(err_msg)
    logger.error(e, exc_info=True)

def main(bucket_data_name, vesselImo, voyage, port, last_pod_seq_num, timestamp, description, path, simulation_id):  
#def main(sourcebucketname, event):
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger_config = logging.getLogger(__name__)
    logger_config.setLevel(logging.INFO)
    global logger
    logger = CustomAdapter(logger_config, {'simu_id': simulation_id})
    logger.info("start main")
    global s3_resource
    s3_resource = resource('s3')
    
    #convert event to dict before calling MainLayer
    event = {
            "vesselImo": vesselImo,
            "voyage": voyage,
            "port": port,
            "last_pod_seq_num": last_pod_seq_num,
            "timestamp": timestamp,
            "description": description,
            "path": path,
            "simulation_id": simulation_id,
 
            }
 
    #event = json.loads(event)
    logger.info(event)
    reusePreviousResults = True
    try:
        main_layer = MainLayer(logger, event, reusePreviousResults, bucket_data_name)
        main_layer.run_main()
        logger.info("Post processing OK")
        
        status_code = 200
    
    except Exception as e:
        err_msg = f"There was an error while running the post processing..."
        log_error(e, err_msg)

        status_code = 400

    return {
        "statusCode": status_code
    }

if __name__ == "__main__":
    # logger.info("getting args")
    # logger.info(sys.argv[1]) # prints var1
    # logger.info(sys.argv[2]) # prints var2
    main(bucket_data_name, vesselImo, voyage, port, last_pod_seq_num, timestamp, description, path, simulation_id)

