import requests
import boto3
from botocore.exceptions import ClientError, BotoCoreError


# === CONFIGURATION ===
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
target_bucket = 'nyc-bronze'          # Target S3 bucket
s3_data_prefix = "raw/"                  # Parquet files folder prefix


# Initialize S3 client
s3 = boto3.client('s3')




def stream_upload_to_s3(file_url: str, s3_bucket: str, s3_key: str):
   file_name = file_url.split('/')[-1]
   try:
       print(f"📥 Downloading: {file_name}")
       response = requests.get(file_url, stream=True, timeout=60)
       response.raise_for_status()


       print(f"📤 Uploading to s3://{s3_bucket}/{s3_key}")
       s3.upload_fileobj(response.raw, s3_bucket, s3_key)
       print(f"✅ Uploaded to s3://{s3_bucket}/{s3_key}")
   except (ClientError, BotoCoreError, requests.RequestException) as e:
       print(f"❌ ERROR uploading {file_name}: {e}")
       raise




def lambda_handler(event, context):
   try:
       # Upload Parquet files (for Jan only as per your range)
       for month in range(1, 13):
           month_str = f"{month:02d}"
           file_name = f"yellow_tripdata_2016-{month_str}.parquet"
           file_url = f"{base_url}/{file_name}"
           s3_key = f"{s3_data_prefix}{file_name}"


           stream_upload_to_s3(file_url, target_bucket, s3_key)


       return {
           "statusCode": 200,
           "body": "✅ Parquet file uploaded successfully to S3."
       }


   except Exception as e:
       print(f"❌ Lambda function failed: {e}")
       return {
           "statusCode": 500,
           "body": f"❌ Lambda failed with error: {str(e)}"
       }
