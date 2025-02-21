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

    def create_bucket(self, bucket_name:str) -> bool:
        """
        Create a new S3 bucket in the specified region.

        :param bucket_name: The name of the bucket to create
        :return: True if the bucket was created, False if there was an error
        :param bucket_name:
        :return:
        """
        # Check if the bucket already exists
        list_of_buckets = self.client.list_buckets().get("Buckets")

        # If the bucket name already exists, modify the name to make it unique
        if bucket_name in [bucket.get("Name") for bucket in list_of_buckets]:
            name = str(datetime.now()).split()[0]
            name = "".join(name.split("-"))
            bucket_name = f"{bucket_name}-{name}"

        # Print the new bucket name for clarity
        print(f"Bucket name: {bucket_name}")

        # Get the AWS region
        region = os.getenv("AWS_REGION", "us-east-1")

        try:
            # Create the bucket with location constraint if the region is not us-east-1
            if region == "us-east-1":
                self.client.create_bucket(Bucket=bucket_name)
            else:
                self.client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"Bucket {bucket_name} created successfully in region {region}.")
            return True
        except Exception as e:
            print(f"Error creating bucket: {str(e)}")
            return False

    def delete_bucket(self, bucket_name: str) -> bool:
        """
        Delete an S3 bucket.

        :param bucket_name: The S3 bucket name
        :return: True if the bucket was deleted, else False
        """

        try:
            self.client.delete_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} deleted successfully.")
            return True
        except Exception as e:
            print(f"Error deleting bucket: {str(e)}")
        return False

    def read_file_to_df(self, bucket_name: str, key: str) -> Union[pd.DataFrame, None]:
        """Reads a CSV file from an S3 bucket and returns it as a pandas DataFrame."""
        try:
            print(f"Fetching file '{key}' from bucket '{bucket_name}'...")
            # Fetch the object from S3
            response = self.client.get_object(Bucket=bucket_name, Key=key)

            # Print response metadata
            print(f"Response Metadata: {response['ResponseMetadata']}")
            print(f"Content-Type: {response.get('ContentType')}")

            # Check if the response status code indicates success (HTTP 200)
            status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            if status_code != 200:
                raise ValueError(f"Failed to retrieve object. Status code: {status_code}")

            # Read the CSV file from the response body
            df = pd.read_csv(response['Body'])

            print("File successfully loaded into DataFrame.")
            print(df.head())  # Print first few rows
            return df

        except ClientError as e:
            # Specific error handling for AWS S3 client errors
            print(f"S3 ClientError: {e}")
        except ValueError as e:
            # Handle case where response status code is not 200
            print(f"ValueError: {e}")
        except Exception as e:
            # Generic exception handling for any other errors
            print(f"An unexpected error occurred: {e}")

        return None  # Return None if an error occurs

    def delete_file(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete a file from an S3 bucket.

        :param bucket_name: The S3 bucket name
        :param object_name: The file to delete
        :return: True if the file was deleted, else False
        """

        try:
            self.client.delete_object(Bucket=bucket_name, Key=object_name)
            print(f"File {object_name} deleted from {bucket_name}")
            return True
        except Exception as e:
            print(f"Error deleting file: {str(e)}")
        return False

    def get_object_metadata(self, bucket_name: str, key: str) -> Union[dict, None]:
        """
        Get metadata for an object in S3.
        - check if file exists, last modified date, file size, content type

        :param bucket_name: The S3 bucket name
        :param key: The object in the bucket
        :return: Metadata dictionary or None if error occurred
        """
        #self.client = get_client("s3")

        try:
            response = self.client.head_object(Bucket=bucket_name, Key=key)
            metadata = {
                "File Size (Bytes)": response.get("ContentLength"),
                "Last Modified": response.get("LastModified"),
                "Content Type": response.get("ContentType"),
                "Storage Class": response.get("StorageClass"),
                "ETag": response.get("ETag"),
                "Metadata": response.get("Metadata"),
            }
            #print(f"Metadata for {key}: {response}")
            print(metadata)
            return metadata
        except Exception as e:
            print(f"Error getting metadata: {str(e)}")
        return None


if __name__ == "__main__":
    # Instantiate the class
    conn = S3Connection()


    # create s3 bucket
    conn.create_bucket("marchbucket2025")
    # delete s3 bucket
    conn.delete_bucket("marchbucket2025")
    # list all the buckets in S3
    print(conn.get_all_buckets())
    # Read CSV file from an S3 bucket and returns it as a pandas DataFrame
    df = conn.read_file_to_df('loanapprovalproject', "Loan_Data.csv")
    print(df.head())
    # upload dataset to S3
    conn.upload_to_s3("s3docupload", "housepriceproject", "dev/train/testupload25")
    # delete file
    conn.delete_file("housepriceproject", "dev/traintestupload25", )
    # get object metadata
    conn.get_object_metadata("housepriceproject", "dev/train2/house_price.csv")

