# raw_to_bronze


import os
import snowflake.connector
from botocore.exceptions import ClientError, BotoCoreError


def lambda_handler(event, context):
   try:
       # Snowflake credentials from Lambda environment variables
       conn = snowflake.connector.connect(
           user=os.environ['user'],
           password=os.environ['password'],
           account=os.environ['account'],  # Option 4: Full URL
          
           warehouse=os.environ['warehouse'],
           database=os.environ['database'],
           schema=os.environ['schema'],
           role=os.environ['role'],
         
           client_session_keep_alive=True,
           connect_timeout=15,
           # insecure_mode=True  # TEMPORARY for testing - remove for production!
       )


       cursor = conn.cursor()


       print("Running COPY INTO command for Bronze table...")


       copy_query = """
       COPY INTO TAXI_DB_PRACTICE.BRONZE.RAW
       FROM (
         SELECT
           $1:VendorID::BIGINT,
           TO_TIMESTAMP_TZ($1:tpep_pickup_datetime::STRING),
           TO_TIMESTAMP_TZ($1:tpep_dropoff_datetime::STRING),
           $1:passenger_count::BIGINT,
           $1:trip_distance::DOUBLE,
           $1:RatecodeID::BIGINT,
           $1:store_and_fwd_flag::STRING,
           $1:PULocationID::BIGINT,
           $1:DOLocationID::BIGINT,
           $1:payment_type::BIGINT,
           $1:fare_amount::DOUBLE,
           $1:extra::DOUBLE,
           $1:mta_tax::DOUBLE,
           $1:tip_amount::DOUBLE,
           $1:tolls_amount::DOUBLE,
           $1:improvement_surcharge::DOUBLE,
           $1:total_amount::DOUBLE,
           $1:congestion_surcharge::INT,
           $1:airport_fee::INT
         FROM @ext1_stage_taxi_s3_bronze
       )
       FILE_FORMAT = (TYPE = 'PARQUET');
       """


       cursor.execute(copy_query)
       print("✅ COPY INTO Bronze executed successfully")


       return {
           "statusCode": 200,
           "body": "Data copied into Bronze layer in Snowflake successfully."
       }


   except Exception as e:
       print(f"❌ ERROR during COPY INTO Bronze: {e}")
       return {
           "statusCode": 500,
           "body": f"Failed to COPY INTO Bronze: {e}"
       }


   finally:
       if 'cursor' in locals():
           cursor.close()
       if 'conn' in locals():
           conn.close()