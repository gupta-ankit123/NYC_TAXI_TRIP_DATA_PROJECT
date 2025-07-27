# You can read more about our project at https://nyctaxitripdataproject-9xjsimuzujwu7wajbm9npb.streamlit.app/
# NYC Taxi Data Pipeline

This project builds a scalable data pipeline to ingest, clean, and analyze the NYC Yellow Taxi Trip dataset (2016), using tools like AWS S3, Lambda, Airflow, Snowflake, and PySpark.

---

## 🚀 Project Overview

This pipeline does the following:
1. Uploads raw taxi trip data to an S3 bucket using AWS Lambda.
2. Transforms and cleans data using PySpark on AWS Glue.
3. Loads cleaned data into Snowflake for downstream analytics.

---

## 🗂️ Datasets Used

### 1. Yellow Taxi Trip Data (2016)
- Source: NYC OpenData
- Contains trip details: pickup/dropoff times, distance, fare, payment type, etc.

### 2. Taxi Zone Lookup Table
- Maps location IDs to boroughs, zones, and service zones.

---

## 🔍 Exploratory Data Analysis (EDA)

Performed EDA to:
- Identify missing values
- Detect zero/negative values in numeric columns
- Find and remove duplicate entries
- Validate payment logic (e.g., tip amount with payment type)
- Identifying outliers and handling it.

---

## 🛠️ Data Transformations

| Step                        | Description                                                                 |
|----------------------------|-----------------------------------------------------------------------------|
| ✅ Dataset Join            | Combined yellow trip data with zone lookup for better readability           |
| ✅ Remove Duplicates       | Dropped exact duplicates from raw dataset                                   |
| ✅ Handle Negatives        | Converted negative values (e.g., trip distance, fare) to absolute values     |
| ✅ Handle Missing Values   | Used median imputation and backfilling                                       |
| ✅ Handle Zeros            | Replaced zeros in critical fields using domain rules                         |
| ✅ Rate Code Cleanup       | Removed trips outside NYC zone coverage                                     |
| ✅ Tip Amount Correction   | Fixed incorrect tip amounts for cash payments                                |
| ✅ Outlier Handling        | Removed trips with illogical timestamps or unrealistic distances             |
| ✅ Passenger Count         | Grouped trips with >5 passengers (violates NYC law)                          |
| ✅ Distance Outliers       | Dropped trips <1km (likely canceled), capped long trips >50 miles            |
| ✅ Feature Engineering     | Added fields like speed, trip duration, and split timestamps                 |

---


## 📦 Lambda Functions

1. **raw-s3-ingestion**  
   Downloads NYC taxi data & uploads to AWS S3.

2. **s3raw-to-snowflake-bronze**  
   Loads data from S3 into Snowflake bronze table using `COPY INTO`.

## 💻 Technologies Used

| Tool/Service     | Purpose                             |
|------------------|-------------------------------------|
| **AWS S3**        | Store raw and cleaned datasets      |
| **AWS Lambda**    | Automate file upload and triggers   |
| **AWS Step Functions** | Orchestrate pipeline steps     |
| **AWS Glue**      | Run PySpark transformations         |
| **Snowflake**     | Store final analytics-ready data    |


---

## 🔄 Orchestration with Step Functions

The workflow:
1. Trigger raw data ingestion Lambda
2. Wait for data upload
3. Trigger `COPY INTO` Lambda
4. Trigger Glue job for Silver & Gold transformation

---

## 🧬 Snowflake Transformations

Scripts to create:
- **Bronze Tables** – Raw schema loaded directly
- **Silver Tables** – Cleaned and enriched data
- **Gold Tables** – Analytical tables with KPIs

---

---

## 🏗️ Architecture

![pipeline-diagram](docs/nyc_pipeline_architecture.png)

(*Optional*: Add an architecture diagram showing Lambda → S3 → Step Function → Glue → Snowflake)

---

## 📊 Outputs

- Cleaned 2016 NYC taxi trip dataset
- New features (speed, duration, zone-based mapping)
- Validated and outlier-free data in Snowflake
- Ready for BI dashboards and analytics

---

## 🏁 How to Run

1. **Deploy Lambda Functions**: 
   - Set environment variables for S3 bucket, file URLs, and Snowflake credentials.
   - Deploy using AWS Console or Terraform.

2. **Run Glue Job**:
   - After bronze load, run `gold_layer_etl.py` via AWS Glue

3. **Run Streamlit App**:
   ```bash
   cd streamlit_app
   pip install -r requirements.txt
   streamlit run app.py

## 📌 Future Improvements

- Add CI/CD for DAG and Lambda deployment
- Add Great Expectations for data quality checks
- Automate Snowflake loading with Snowpipe
- Using BI Tools for better analysis

---

## 📎 License

This project is licensed under the [MIT License](LICENSE).



## Team Members:
- Ankit Kumar Gupta
- Gitanshu Aneja
- Mohit Doraiburu
- Ritesh Regar
- Sanjeeb Sethy
- Shatanshu Bodhke
