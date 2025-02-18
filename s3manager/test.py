#s3_operations.py
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from dotenv import load_dotenv
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

print(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)


conn = boto3.client("s3",aws_access_key_id= AWS_ACCESS_KEY_ID,aws_secret_access_key = AWS_SECRET_ACCESS_KEY,)

response = conn.list_buckets()
bucket_list = [bucket["Name"] for bucket in response.get("Buckets", [])]
print(bucket_list)