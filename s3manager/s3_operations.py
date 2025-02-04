#s3_operations.py
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from dotenv import load_dotenv
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


