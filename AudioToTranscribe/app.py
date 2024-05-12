import boto3
import logging
import time
import os
import requests
import json
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

# Initialize the Boto3 client for S3 and Transcribe
s3_client = boto3.client('s3')
transcribe_client = boto3.client('transcribe')

def upload_file_to_s3(file_name, bucket_name):
    try:
        s3_client.upload_file(file_name, bucket_name, file_name)
        logging.info(f"File {file_name} uploaded to {bucket_name}")
        return True
    except ClientError as e:
        logging.error(f"Failed to upload file to S3: {str(e)}")
        return False

def start_transcription(job_name, bucket_name, file_name):
    try:
        transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': f"s3://{bucket_name}/{file_name}"},
            MediaFormat='m4a',
            LanguageCode='hi-IN',  # Set appropriate language code
            Settings={'ShowSpeakerLabels': True, 'MaxSpeakerLabels': 10}
        )
        logging.info("Started transcription job.")
        return True
    except ClientError as e:
        logging.error(f"Failed to start transcription job: {str(e)}")
        return False

def check_transcription(job_name):
    try:
        while True:
            status = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            time.sleep(5)
        if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
            logging.info("Transcription job completed successfully.")
            return status['TranscriptionJob']['Transcript']['TranscriptFileUri']
        else:
            logging.error("Transcription job failed.")
            return None
    except ClientError as e:
        logging.error(f"Failed to check transcription job: {str(e)}")
        return None

def download_and_parse_transcript(transcript_url):
    response = requests.get(transcript_url)
    transcript = response.json()
    speaker_labels = {'spk_0': 'X', 'spk_1': 'Y'}
    formatted_transcript = ""
    current_speaker = None
    transcript_part = ""

    for item in transcript['results']['items']:
        if 'speaker_label' in item:
            speaker = speaker_labels[item['speaker_label']]
            if current_speaker and speaker != current_speaker:
                formatted_transcript += f"{current_speaker}: {transcript_part.strip()}\n"
                transcript_part = item['alternatives'][0]['content'] + " "
            else:
                transcript_part += item['alternatives'][0]['content'] + " "
            current_speaker = speaker

    # Add last part if there's any remaining
    if transcript_part:
        formatted_transcript += f"{current_speaker}: {transcript_part.strip()}"

    return formatted_transcript

def main():
    print("Current Working Directory:", os.getcwd())
    # Building the file path in a portable way:
    file_name = os.path.join(os.getcwd(), 'example.m4a')
    bucket_name = 'poc.audiotranscribe'  # Update with your bucket name
    job_name = 'exampleTranscribeJob_03'  # Choose a unique name for the transcription job

    #if upload_file_to_s3(file_name, bucket_name):
    if True:
        if start_transcription(job_name, bucket_name, file_name):
            result = check_transcription(job_name)
            if result:
                transcript = download_and_parse_transcript(result)
                print("Formatted Transcript:\n", transcript)
            else:
                logging.error("Failed to retrieve transcription result.")
        else:
            logging.error("Failed to start transcription job.")
    else:
        logging.error("Failed to upload file to S3.")

if __name__ == "__main__":
    main()
