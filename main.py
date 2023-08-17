from modules.my_module import function_hello

def lambda_handler(event, context):
    
    function_hello()

    return {
        "statusCode": 200
        }
