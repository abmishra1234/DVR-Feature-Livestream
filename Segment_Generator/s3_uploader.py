import logging
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import concurrent.futures

class S3Uploader:
    def __init__(self, bucket_name, region_name='ap-south-1', max_workers=5):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def upload_file(self, file_path, bucket_name, s3_path):
        try:
            logging.debug(f"Uploading {file_path} to s3://{bucket_name}/{s3_path}")
            self.s3_client.upload_file(file_path, bucket_name, s3_path)
            logging.info(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_path}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logging.error(f"Credentials error when uploading {file_path} to S3: {e}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Error uploading {file_path} to S3: {e}", exc_info=True)
            raise

    def upload_files(self, file_list, bucket_name, s3_prefix):
        futures = []
        for file_path in file_list:
            s3_path = os.path.join(s3_prefix, os.path.relpath(file_path, start=file_list[0])).replace("\\", "/")
            futures.append(self.executor.submit(self.upload_file, file_path, bucket_name, s3_path))
        concurrent.futures.wait(futures)

    def shutdown(self):
        self.executor.shutdown(wait=True)
