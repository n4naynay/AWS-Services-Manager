##################################################################
# Created : Zyn Mab
#################################################################

import os
from typing import Union
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

    def upload_to_s3(self, data: Union[str, pd.DataFrame], bucket_name: str, key: str,
                     is_dataframe: bool = False) -> bool:
        """
         General method to upload either a file or a DataFrame to S3.

        If the file (or object) already exists in the S3 bucket, the method returns `True`.
        If the file does not exist, it uploads the file or DataFrame to the S3 bucket and returns `False`.

        Params:
            data (Union[str, pd.DataFrame]): The input data, which can either be:
                - a local file path (str) when uploading a file, or
                - a pandas DataFrame (pd.DataFrame) when uploading data as CSV.
            bucket_name (str): The name of the S3 bucket where the file or DataFrame should be uploaded.
            key (str): The key (path) for the object in the S3 bucket.
            is_dataframe (bool): Flag indicating whether the `data` is a DataFrame. Defaults to False (indicating `data` is a file).

        Returns:
            bool: `True` if the file already exists in the bucket, `False` if the file is uploaded.

        Raises:
            Exception: If there is an error during the S3 interaction (e.g., invalid credentials, network issues).
        """
        try:
            # Check if the object exists in the bucket
            self.client.head_object(Bucket=bucket_name, Key=key)
            logging.info(f"File {key} already exists in bucket {bucket_name}.")
            return True  # File exists
        except Exception as e:
            logging.info(f"File {key} not found, uploading a new file.")
            if is_dataframe:
                # Handle DataFrame upload
                csv_buffer = StringIO()
                data.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                self.client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
                logging.info(f"DataFrame uploaded as {key} to bucket {bucket_name}.")
            else:
                # Handle file upload
                self.client.upload_file(data, bucket_name, key)  # `data` is a file path here
                logging.info(f"File {data} uploaded as {key} to bucket {bucket_name}.")
            return False  # File does not exist or uploaded



if __name__ == "__main__":
    # Instantiate the class
    conn = S3Connection()
    print(conn.get_all_buckets())
    conn.upload_to_s3("s3docupload","firstprojectnene","testupload22")
