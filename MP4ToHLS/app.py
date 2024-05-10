import subprocess
import boto3
import os

# Function to convert MP4 to HLS using FFmpeg
def convert_to_hls(input_file, output_dir):
    """Converts an MP4 file to HLS format using FFmpeg."""
    command = [
        'ffmpeg', '-i', input_file, '-profile:v', 'baseline', '-level', '3.0',
        '-s', '640x360', '-start_number', '0', '-hls_time', '10', '-hls_list_size', '0',
        '-f', 'hls', f'{output_dir}/index.m3u8'
    ]
    subprocess.run(command, check=True)
    print(f"Conversion completed. HLS files are in {output_dir}")

# Function to upload HLS files to an S3 bucket
def upload_files(bucket_name, source_dir):
    """Uploads files from a directory to an S3 bucket."""
    s3 = boto3.client('s3')
    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith(('.m3u8', '.ts')):
                s3.upload_file(os.path.join(root, file), bucket_name, file)
                print(f"Uploaded {file} to {bucket_name}/{file}")

# Function to create a CloudFront distribution
def create_cloudfront_distribution(bucket_name):
    """Creates a CloudFront distribution for an S3 bucket."""
    cf = boto3.client('cloudfront')
    origin_id = f'S3-{bucket_name}'
    response = cf.create_distribution(DistributionConfig={
        'CallerReference': 'my-distribution',
        'Comment': 'HLS distribution',
        'Enabled': True,
        'Origins': {
            'Quantity': 1,
            'Items': [{
                'Id': origin_id,
                'DomainName': f'{bucket_name}.s3.amazonaws.com',
                'S3OriginConfig': {
                    'OriginAccessIdentity': ''
                }
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
    print("CloudFront Distribution Created: ", response['Distribution']['DomainName'])
    return response['Distribution']['DomainName']

# Main function to coordinate the conversion, upload, and distribution setup
def main():
    input_file = 'input/video.mp4'
    output_dir = 'output'

    # Convert MP4 to HLS
    #convert_to_hls(input_file, output_dir)

    
    bucket_name = 'poc.hls.streams'
    # Upload HLS files to S3
    #upload_files(bucket_name, output_dir)

    # Create CloudFront distribution
    create_cloudfront_distribution(bucket_name)

if __name__ == "__main__":
    main()
