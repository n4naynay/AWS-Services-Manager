##################################################################
# Created : Zyn Mab
#################################################################

import os
from typing import Union
from datetime import datetime
import logging

import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


from dotenv import load_dotenv



class S3Connection:
    def __init__(self):
        """
        initializes S3 client using AWS credentials from the environment variables
        this method loads AWS access keys (access_key, secret_key) from environment variables
        if there is an issue creating S3 connection, it raises a ValueError with error message

        returns None
        """
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        REGION_NAME = os.getenv("REGION_NAME", "US-EAST-1") # Default to "us-east-1" if not set
        print(AWS_ACCESS_KEY_ID,AWS_ACCESS_KEY_ID)


        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
            raise ValueError("AWS credentials are not set in the environment variables")

        try:
            # Create an S3 client using provided credentials
            self.client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                       region_name=REGION_NAME)
        except Exception as e:
            raise ValueError(f"Failed to create S3 client:{e}")

    def get_all_buckets(self) -> list:
        """
        list all S3 bucket.
        :param: none
        :return: a list
        """
        all_buckets = []
        for items in self.client.list_buckets().get("Buckets"):
            all_buckets.append(items.get("Name"))
        return all_buckets


    def upload_file(file_path: str, bucket_name: str, object_name = None) -> Union[str, bool]:
        """
        Uploads a file to an s3 bucket
        :param file_path:loca; path to the file
        :param bucket_name: s3 bucket name
        :param object_name:name of file in the bucket (if none, the file name will be used)
        :return:
        """
        if not object_name:
            object_name = os.path.basename(file_path)

        s3_client = get_client()

if __name__ == "__main__":
    # Instantiate the class
    conn = S3Connection()
    conn.get_all_buckets()
    conn.upload_file("s3docupload","firstprojectnene","testupload")
