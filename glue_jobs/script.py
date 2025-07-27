import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# -----------------------------
# Step 1: Setup Glue Context
# -----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_nyc_taxi = spark.read.parquet(
    "s3://nyc-taxi-bktt/raw1/"
)
df_zone_lookup=spark.read.format('csv').option("header",True).option('inferSchema',True).load('s3://nyc-taxi-bktt/zone-lookup/')
df_zone_lookup_filled =  df_zone_lookup.replace("N/A", "Unknown").fillna("Unknown")
df_zone_lookup = df_zone_lookup.dropDuplicates(["LocationID"])
df_zone_lookup_PU=df_zone_lookup.select([col(c).alias(f"PU{c}") if c != "LocationID" else col(c).alias("PULocationID") for c in df_zone_lookup.columns])
df_zone_lookup_DO=df_zone_lookup.select([col(c).alias(f"DO{c}") if c != "LocationID" else col(c).alias("DOLocationID") for c in df_zone_lookup.columns])

# Join pickup location data
df_joined = df_nyc_taxi.join(df_zone_lookup_PU, on="PULocationID", how="left")

# Join dropoff location data
df_joined = df_joined.join(df_zone_lookup_DO, on="DOLocationID", how="left")

columns_to_drop = [
    'congestion_surcharge','airport_fee'
]

df_joined = df_joined.drop(*columns_to_drop)

df_joined = df_joined.dropDuplicates()

from pyspark.sql.window import Window

# Define the subset columns used to detect duplicates
subset_cols = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "RatecodeID",
    "PULocationID", "DOLocationID", "passenger_count", "trip_distance"
]

# Define a window partitioned by subset columns
window_spec = Window.partitionBy([col(c) for c in subset_cols])

# Add a count column that counts how many rows share the same values in subset_cols
df_with_dup_count = df_joined.withColumn("dup_count", count("*").over(window_spec))

# Filter rows where dup_count > 1 (i.e., duplicates including the first occurrence)
duplicate_value = df_with_dup_count.filter(col("dup_count") > 1).drop("dup_count")

# Filter negative fare_amount values from duplicates
dup_negatif_val = duplicate_value.filter(col("fare_amount") < 0)

# joins the two DataFrames and keeps only those rows from df_joined that do not exist in dup_negatif_val.
df_joined = df_joined.join(dup_negatif_val, on=df_joined.columns, how='left_anti')

# Replace negative fare_amounts with their absolute value
df_joined = df_joined.withColumn(
    "fare_amount",
    when(col("fare_amount") < 0, abs(col("fare_amount"))).otherwise(col("fare_amount"))
)

from pyspark.sql.functions import col, expr

# Step 1: Calculate median (approximate) for passenger_count
median_passenger_count = df_joined.approxQuantile("passenger_count", [0.5], 0.01)[0]

# Step 2: Replace passenger_count = 0 with median
df_joined = df_joined.withColumn(
    "passenger_count",
    expr(f"CASE WHEN passenger_count = 0 THEN {median_passenger_count} ELSE passenger_count END")
)

# Step 3: Filter rows with trip_distance > 0
df_joined = df_joined.filter(col("trip_distance") > 0)

# Step 4: Filter rows with fare_amount > 0
df_joined = df_joined.filter(col("fare_amount") > 0)

df_joined = df_joined.replace("N/A", "Unknown")

# Apply conditional updates to RatecodeID
df_joined = df_joined.withColumn(
    "RatecodeID",
    when(
        (col("RatecodeID") == 4) &
        (col("PUBorough") != "Unknown") &
        (col("DOBorough") == "EWR"),
        3
    ).when(
        (col("RatecodeID") == 4) &
        (col("PUBorough") != "Unknown") &
        (col("DOBorough") != "Unknown"),
        1
    ).otherwise(col("RatecodeID"))
)


# Remove rows where either PUBorough or DOBorough is 'Unknown'
df_joined = df_joined.filter(
    (col("PUBorough") != "Unknown") & (col("DOBorough") != "Unknown")
)

# Step 1: Replace 99 with null in RatecodeID
df_joined = df_joined.withColumn(
    "RatecodeID",
    when(col("RatecodeID") == 99, None).otherwise(col("RatecodeID"))
)

# Step 2: Update RatecodeID based on custom conditions
df_joined = df_joined.withColumn(
    "RatecodeID",
    when(
        (col("RatecodeID").isNull()) &
        (col("PUBorough") == "Manhattan") &
        (col("DOZone") == "JFK Airport"),
        2
    ).when(
        (col("RatecodeID").isNull()) &
        (col("PUZone") == "JFK Airport") &
        (col("DOBorough") == "Manhattan"),
        2
    ).when(
        (col("RatecodeID").isNull()) &
        (col("DOZone") == "Newark Airport"),
        3
    ).otherwise(col("RatecodeID"))
)

# Fill nulls in RatecodeID with 1
df_joined = df_joined.withColumn(
    "RatecodeID",
    when(col("RatecodeID").isNull(), 1).otherwise(col("RatecodeID"))
)

# Step 1: Calculate median of passenger_count
median_value = df_joined.approxQuantile("passenger_count", [0.5], 0.0)[0]

# Step 2: Fill nulls with median value
df_joined = df_joined.withColumn(
    "passenger_count",
    when(col("passenger_count").isNull(), median_value).otherwise(col("passenger_count"))
)

# Step 1: Update payment_type from 2 to 1 where tip_amount > 0
df_joined = df_joined.withColumn(
    "payment_type",
    when((col("tip_amount") > 0) & (col("payment_type") == 2), 1).otherwise(col("payment_type"))
)

df_joined = df_joined.withColumn(
    "payment_type",
    when(isnull(col("payment_type")), when(col("tip_amount") > 0, 1.0).otherwise(5.0))
    .otherwise(col("payment_type"))
)
df_joined = df_joined.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
df_joined = df_joined.withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))

outliers = df_joined.filter(
    ~(
        (year(col("tpep_pickup_datetime")) == 2016) &
        (month(col("tpep_pickup_datetime")).isin([1, 2, 3,4,5,6,7,8,9,10,11,12]))
    )
)
df_joined = df_joined.withColumn(
    "RatecodeID",
    when(col("RatecodeID") == 1, "Standard rate")
    .when(col("RatecodeID") == 2, "JFK Airport")
    .when(col("RatecodeID") == 3, "Newark Airport")
    .when(col("RatecodeID") == 4, "Nassau or Westchester")
    .when(col("RatecodeID") == 5, "Negotiated fare")
    .when(col("RatecodeID") == 6, "Group ride")
    .otherwise(col("RatecodeID"))  # keep original if none of above
)

# Convert passenger_count to a string and group values >5 into '>5'
df_joined = df_joined.withColumn(
    "passenger_count",
    when(col("passenger_count") > 5, ">5").otherwise(col("passenger_count").cast("string"))
)
# Count number of rows with trip distance less than 0.62
count_short_trips = df_joined.filter(col("trip_distance") < 0.62).count()
print("Number of records with trip distance < 1 km (0.62 miles):", count_short_trips)

# Filter out short-distance trips
df_joined = df_joined.filter(col("trip_distance") > 0.62)

# Filter rows where trip_distance > 50 and select columns
outliers = df_joined.filter(col("trip_distance") > 50) \
                    .select("PULocationID", "DOLocationID", "trip_distance")

# Filter out extreme outliers (trip_distance < 50)
df_filtered = df_joined.filter(col('trip_distance') < 50)

# Compute count, average, and median of trip_distance grouped by PULocationID and DOLocationID
ct_distance_byid = df_filtered.groupBy('PULocationID', 'DOLocationID').agg(
    count('trip_distance').alias('count'),
    mean('trip_distance').alias('avg_distance'),
    expr('percentile_approx(trip_distance, 0.5)').alias('median_distance')
)

# Join outliers with median distance table on PU and DO location IDs
distance_byid = outliers.join(
    ct_distance_byid.drop('count'), 
    on=['PULocationID', 'DOLocationID'],
    how='left'
)
# Join df_joined with distance_byid to get median values where needed
df_with_median = df_joined.join(
    distance_byid.select('PULocationID', 'DOLocationID', 'median_distance'),
    on=['PULocationID', 'DOLocationID'],
    how='left'
)

# Replace trip_distance with median_distance for outliers (where trip_distance > 50)
df_imputed = df_with_median.withColumn(
    'trip_distance',
    when(col('trip_distance') > 50, col('median_distance')).otherwise(col('trip_distance'))
).drop('median_distance')

df_joined = df_imputed
isna = df_joined.filter(col("trip_distance").isNull()) \
                .select("PUBorough", "DOBorough", "trip_distance")

# Filter data with trip_distance < 70
df_filtered = df_joined.filter(col("trip_distance") < 70)

# Group by PUBorough and DOBorough and compute count, mean, median
ct_distance_byborough = df_filtered.groupBy("PUBorough", "DOBorough").agg(
    count("trip_distance").alias("count"),
    avg("trip_distance").alias("avg_distance"),
    expr("percentile_approx(trip_distance, 0.5)").alias("median_distance")
)
# Step 2: Join with ct_distance_byborough to get median_distance for each borough pair
distance_byborough = isna.join(
    ct_distance_byborough.select("PUBorough", "DOBorough", "median_distance"),
    on=["PUBorough", "DOBorough"],
    how="left"
)

# Step 3: Perform the imputation (replace null trip_distance with median_distance)
df_with_median = df_joined.join(
    ct_distance_byborough.select("PUBorough", "DOBorough", "median_distance"),
    on=["PUBorough", "DOBorough"],
    how="left"
)

df_imputed = df_with_median.withColumn(
    "trip_distance",
    when(col("trip_distance").isNull(), expr("round(median_distance, 2)"))
    .otherwise(col("trip_distance"))
).drop("median_distance") 

df_joined = df_imputed

df_joined = df_joined.withColumn(
    "trip_duration",
    round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
)

# Calculate speed in miles per hour
df_joined = df_joined.withColumn(
    "speed",
    round(col("trip_distance") / (col("trip_duration") / 60), 2)
)

# 1. Create pickup_date (DateType) and pickup_time (String)
df_joined = df_joined.withColumn("pickup_date", to_date("tpep_pickup_datetime"))
df_joined = df_joined.withColumn("pickup_time", date_format("tpep_pickup_datetime", "HH:mm:ss"))

# 2. Create dropoff_date (DateType) and dropoff_time (String)
df_joined = df_joined.withColumn("dropoff_date", to_date("tpep_dropoff_datetime"))
df_joined = df_joined.withColumn("dropoff_time", date_format("tpep_dropoff_datetime", "HH:mm:ss"))

# 3. Convert both timestamps to nanosecond-format strings
df_joined = df_joined.withColumn(
    "tpep_pickup_datetime",
    date_format("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
)

df_joined = df_joined.withColumn(
    "tpep_dropoff_datetime",
    date_format("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")
)

df_joined=df_joined.dropDuplicates()

# Write cleaned data to S3 in Parquet format
output_path = "s3://process-bkt-03/processed/"
df_joined.write.mode("overwrite").parquet(output_path)
job.commit()