import boto3
from botocore.exceptions import NoCredentialsError,ClientError
import os
from dotenv import load_dotenv
#load credentials from .env file
load_dotenv()
def get_s3_client():
    try:
        client = boto3.client(
            's3',
           aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name = os.getenv('AWS_REGION'),
            bucket_name = os.getenv('S3_BUCKET')
        )
        print("s3 client created successfully")
        return client
    except NoCredentialsError:
        print("Credentials not found.please check your .env file")
    except Exception as e:
        print(f"An error occurred while creating s3 client:{e}")
def upload_file_to_s3(file_path, bucket_name, object_name=None):
    s3_client = get_s3_client()
    if s3_client is None:
        print("s3 client is not available. file upload failed.")
        return False
    if object_name is None:
        object_name = os.path.basename(file_path)
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"file {file_path} uploaded successfully to bucket {bucket_name} as {object_name}")
        return True
    except ClientError as e:
        print(f"An error occurred while uploading the file: {e}")
        return False