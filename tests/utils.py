import boto3
import os


env_account_mapping = dict(
    int="shipping-noprd",
    uat="shipping-noprd",
    pre="shipping-noprd",
    prd="shipping-prd"
)
role_name = "data_scientist_access"
referential_bucket_name = "optistow-referential-bucket"

def boto_session(environment: str="prd"):
    profile = f"{env_account_mapping.get(environment)}.{role_name}"
    print(profile)
    session = boto3.Session(profile_name=profile)
    return session

def get_referential_files_from_s3(local_simulation_folder: str, bucket: str=referential_bucket_name, prefix: str="", environment="prd"):
    session = boto_session(environment)
    s3 = session.client("s3")
    bucket = f"{bucket}-{environment}"

    # get referential files list from s3
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response['Contents']

    # copy all referential files in local folder
    referential_local_folder = f"{local_simulation_folder}/referential"
    os.makedirs(referential_local_folder, exist_ok=True)
    for obj in objects:
        obj_key = obj.get("Key")
        obj_local_path = f"{referential_local_folder}/{obj_key}"
        os.makedirs(os.path.dirname(obj_local_path), exist_ok=True)
        print(obj_key)
        s3.download_file(bucket, obj_key, obj_local_path)
    