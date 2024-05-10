import subprocess
import boto3
import os
import logging

# Import the time module
import time  

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MediaConverter:
    """Class to handle media conversion from MP4 to HLS using FFmpeg."""
    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.output_dir = output_dir

    def convert_to_hls(self):
        """Converts an MP4 file to HLS format using FFmpeg."""
        command = [
            'ffmpeg', '-i', self.input_file, '-profile:v', 'baseline', '-level', '3.0',
            '-s', '640x360', '-start_number', '0', '-hls_time', '10', '-hls_list_size', '0',
            '-f', 'hls', f'{self.output_dir}/index.m3u8'
        ]
        try:
            subprocess.run(command, check=True)
            logging.info("Conversion completed. HLS files are in %s", self.output_dir)
        except subprocess.CalledProcessError as e:
            logging.error("Failed to convert video. Error: %s", e)

class S3Uploader:
    """Class to handle file uploading to AWS S3."""
    def __init__(self, bucket_name, source_dir):
        self.bucket_name = bucket_name
        self.source_dir = source_dir
        self.s3 = boto3.client('s3')

    def upload_files(self):
        """Uploads files from a directory to an S3 bucket."""
        try:
            for root, _, files in os.walk(self.source_dir):
                for file in files:
                    if file.endswith(('.m3u8', '.ts')):
                        self.s3.upload_file(os.path.join(root, file), self.bucket_name, file)
                        logging.info("Uploaded %s to %s/%s", file, self.bucket_name, file)
        except Exception as e:
            logging.error("Failed to upload files. Error: %s", e)

class CloudFrontManager:
    """Class to manage AWS CloudFront distributions."""
    def __init__(self, bucket_name, distribution_id=None):
        self.bucket_name = bucket_name
        self.distribution_id = distribution_id
        self.cf = boto3.client('cloudfront')

    def get_distribution(self):
        """Checks if the specified distribution ID exists and returns it."""
        if self.distribution_id:
            try:
                response = self.cf.get_distribution(Id=self.distribution_id)
                logging.info("Existing distribution retrieved: %s", self.distribution_id)
                return response['Distribution']
            except self.cf.exceptions.NoSuchDistribution:
                logging.warning("No such distribution found with ID: %s", self.distribution_id)
                return None
        else:
            logging.info("No distribution ID provided. Proceeding to create a new one.")
            return None

    def create_distribution(self):
        """Creates a CloudFront distribution for an S3 bucket if none exists."""
        if not self.get_distribution():
            origin_id = f'S3-{self.bucket_name}'
            response = self.cf.create_distribution(DistributionConfig={
                'CallerReference': 'hls_distribution',
                'Comment': 'HLS distribution',
                'Enabled': True,
                'Origins': {
                    'Quantity': 1,
                    'Items': [{
                        'Id': origin_id,
                        'DomainName': f'{self.bucket_name}.s3.amazonaws.com',
                        'S3OriginConfig': {'OriginAccessIdentity': ''}
                    }]
                },
                'DefaultCacheBehavior': {
                    'TargetOriginId': origin_id,
                    'ViewerProtocolPolicy': 'redirect-to-https',
                    'AllowedMethods': {
                        'Quantity': 2,
                        'Items': ['GET', 'HEAD'],
                        'CachedMethods': {
                            'Quantity': 2,
                            'Items': ['GET', 'HEAD']
                        }
                    },
                    'ForwardedValues': {
                        'QueryString': False,
                        'Cookies': {'Forward': 'none'}
                    },
                    'MinTTL': 86400
                }
            })
            new_distribution_id = response['Distribution']['Id']
            logging.info("New CloudFront Distribution Created: %s", new_distribution_id)
            return new_distribution_id
        return self.distribution_id

    def invalidate_cache(self):
        """Invalidates CloudFront distribution cache if S3 content changes."""
        if self.distribution_id:
            try:
                response = self.cf.create_invalidation(
                    DistributionId=self.distribution_id,
                    InvalidationBatch={
                        'Paths': {
                            'Quantity': 1,
                            'Items': ['/*']
                        },
                        'CallerReference': str(time.time())  # Use timestamp as a unique reference
                    }
                )
                logging.info("Cache invalidated for distribution ID: %s", self.distribution_id)
            except Exception as e:
                logging.error("Failed to invalidate cache. Error: %s", e)

def main():
    input_file = 'input/video.mp4'
    output_dir = 'output'
    converter = MediaConverter(input_file, output_dir)

    bucket_name = 'poc.hls.streams'
    # Replace with your actual ID or None if you want to create a new distribution
    distribution_id = 'E3307LF0PMAAC5'

    uploader = S3Uploader(bucket_name, output_dir)
    cf_manager = CloudFrontManager(bucket_name, distribution_id)

    converter.convert_to_hls()
    uploader.upload_files()

    # This will check for existing ID or create a new one
    cf_manager.create_distribution()
    
    # Invalidate cache after ensuring distribution is set up
    cf_manager.invalidate_cache()
    print("Using CloudFront Distribution:", distribution_id)

if __name__ == "__main__":
    main()
