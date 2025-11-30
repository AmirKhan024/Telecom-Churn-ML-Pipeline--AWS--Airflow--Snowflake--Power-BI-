# 🚀 Customer Churn Prediction - End-to-End Data Engineering & ML Pipeline

[![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20EC2-orange)](https://aws.amazon.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://www.python.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=powerbi)](https://powerbi.microsoft.com/)


---

## 🎯 Project Overview

An end-to-end **hybrid data engineering pipeline** that combines **event-driven architecture** with **orchestration monitoring** to predict customer churn for a telecommunications company. The project ingests raw customer data, performs ETL transformations, trains a Random Forest ML model, loads data into Snowflake, and visualizes insights in Power BI.

### Business Problem
Customer churn is a critical issue for telecom companies. This project:
- ✅ Predicts which customers are likely to churn
- ✅ Identifies key factors influencing churn
- ✅ Provides actionable insights through interactive dashboards
- ✅ Automates the entire ML pipeline from data ingestion to visualization

### Key Achievements
- **77.43% Model Accuracy** - Random Forest classification model
- **Real-time Processing** - Event-driven architecture with AWS Lambda
- **Automated Pipeline** - Zero manual intervention after initial setup
- **Scalable Design** - Handles increasing data volumes seamlessly
- **Production Ready** - Monitoring, logging, and error handling

---

## 🏗️ Architecture

### Hybrid Event-Driven + Orchestrated Architecture
<img width="2593" height="809" alt="diagram-export-11-30-2025-1_37_27-PM" src="https://github.com/user-attachments/assets/7d071931-1cee-4015-970b-bc40625edc82" />


---

## 🛠️ Tech Stack


**Cloud & Infrastructure:**
- AWS S3: Data lake storage (raw, transformed, ML models)
- AWS Lambda: Serverless compute for ETL and ML training
- AWS EC2: Airflow orchestration server
- AWS CloudWatch: Logging and monitoring


**Data Processing & ML:**
- Python 3.12: Primary programming language
- Pandas: Data manipulation and cleaning
- Scikit-learn: Machine learning 
- NumPy: Numerical computations


**Orchestration & Workflow:**
- Apache Airflow: Workflow orchestration and monitoring
- Boto3: AWS SDK for Python


**Data Warehouse & Analytics:**
- Snowflake: Cloud data warehouse
- Snowpipe: Continuous data ingestion
- Power BI: Business intelligence and visualization

---

## ✨ Features

### 🔄 Automated ETL Pipeline
- **Event-Driven**: Automatically triggered by S3 file uploads
- **Data Cleaning**: Handles missing values, type conversions
- **Feature Engineering**: Creates derived features (tenure groups, charge ratio)
- **Encoding**: Binary and categorical encoding for ML

### 🤖 Machine Learning
- **Algorithm**: Random Forest Classifier
- **Performance**: 77.43% accuracy for churn detection
- **Feature Importance**: Identifies key churn drivers (Contract type, Tenure, Monthly charges)
- **Model Versioning**: Timestamped model artifacts in S3

### 📊 Real-Time Data Warehouse
- **Auto-Ingestion**: Snowpipe continuously loads data from S3
- **Dual Tables**: Encoded for ML, decoded for analytics
- **Data Quality**: Automated validation and checks
- **Scalable**: Handles millions of rows efficiently

### 📈 Interactive Dashboards
- **9 Visualizations**: Comprehensive churn analysis
- **Customer Segmentation**: Risk categorization (High/Medium/Low)
- **Financial Metrics**: Revenue impact analysis
- **Real-Time Updates**: Live data from Snowflake

### 🔍 Monitoring & Observability
- **Airflow Dashboard**: Visual pipeline status
- **Centralized Logging**: All components log to CloudWatch
- **Error Handling**: Retry logic and failure notifications
- **Historical Tracking**: Audit trail of all executions

---

## 🚀 Setup Instructions

### Prerequisites

- **AWS Account** with permissions for S3, Lambda, EC2, CloudWatch
- **Snowflake Account** 
- **Python 3.11+**
- **Power BI Desktop** (for dashboard visualization)
- **Git**

---

### Step 1: AWS Infrastructure Setup

#### 1.1 Create S3 Buckets

```bash
# Create three S3 buckets
aws s3 mb s3://first-customer-churn-raw
aws s3 mb s3://second-customer-churn-transformed
aws s3 mb s3://third-customer-churn-ml-model

# Verify buckets created
aws s3 ls
```

#### 1.2 Create Lambda 1 (Data Transformation)

1. Go to **AWS Lambda Console** → **Create function**
2. **Function name**: `transform-customer-churn-data`
3. **Runtime**: Python 3.10
4. **Architecture**: x86_64
5. Copy code from `lambda/lambda1_transform/lambda_function.py`
6. **Configuration**:
   - Memory: 512 MB
   - Timeout: 2 minutes
7. **Add Layer**: AWSSDKPandas-Python310
8. **Add S3 Trigger**:
   - Bucket: `first-customer-churn-raw`
   - Event type: PUT
   - Suffix: `.csv`

#### 1.3 Create Lambda 2 (ML Training)

1. **Create sklearn layer** (locally):
```bash
mkdir sklearn-layer
cd sklearn-layer
mkdir python
cd python
pip install scikit-learn numpy scipy -t .
cd ..
zip -r sklearn-layer.zip python
```

2. **Upload layer to Lambda**:
   - Lambda Console → Layers → Create layer
   - Upload `sklearn-layer.zip`

3. **Create Lambda function**:
   - Function name: `train-churn-ml-model`
   - Runtime: Python 3.10
   - Memory: 1024 MB
   - Timeout: 5 minutes
   - Attach sklearn layer
   - Copy code from `lambda/lambda2_ml_training/lambda_function.py`

4. **Add S3 Trigger**:
   - Bucket: `second-customer-churn-transformed`
   - Event type: PUT
   - Prefix: `ml/`
   - Suffix: `_transformed.csv`

#### 1.4 Configure IAM Permissions

Add this policy to both Lambda execution roles:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::first-customer-churn-raw",
                "arn:aws:s3:::first-customer-churn-raw/*",
                "arn:aws:s3:::second-customer-churn-transformed",
                "arn:aws:s3:::second-customer-churn-transformed/*",
                "arn:aws:s3:::third-customer-churn-ml-model",
                "arn:aws:s3:::third-customer-churn-ml-model/*"
            ]
        }
    ]
}
```

---

### Step 2: Airflow Setup on EC2

#### 2.1 Launch EC2 Instance

```bash
# Instance specifications:
- AMI: Amazon Linux 2023
- Instance type: t2.medium (2 vCPU, 4 GB RAM)
- Storage: 20 GB
- Security group: Allow SSH (22), HTTP (8080)
```

#### 2.2 Install and Configure Airflow

```bash
# SSH into EC2
ssh -i your-key.pem ec2-user@your-ec2-ip

# Create virtual environment
python3 -m venv first_venv
source first_venv/bin/activate

# Install Airflow
pip install apache-airflow

# Install dependencies
pip install boto3 pandas numpy scikit-learn joblib requests

# Initialize Airflow
airflow standalone

# Note down the admin username and password from output
```

#### 2.3 Deploy DAG

```bash
# Create dags folder
mkdir -p ~/airflow/dags

# Copy DAG file
cp customer_churn_etl_dag.py ~/airflow/dags/

# Configure AWS credentials
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Enter region: ap-south-1 (or your region)

# Restart Airflow
pkill -f airflow
airflow standalone
```

#### 2.4 Access Airflow UI

```
URL: http://your-ec2-public-ip:8080
Login with credentials from standalone output
```

---

### Step 3: Snowflake Setup

#### 3.1 Create Database and Schema

```sql
-- Run in Snowflake worksheet
CREATE DATABASE telecom_churn_db;
USE DATABASE telecom_churn_db;

CREATE SCHEMA churn_schema;
CREATE SCHEMA file_format_schema;
CREATE SCHEMA external_stage_schema;
CREATE SCHEMA snowpipe_schema;
```

#### 3.2 Create Tables

```sql
-- Encoded table (for ML)
CREATE OR REPLACE TABLE churn_schema.churn_transformed_encoded (
    gender INT,
    SeniorCitizen INT,
    Partner INT,
    Dependents INT,
    tenure INT,
    PhoneService INT,
    MultipleLines INT,
    InternetService INT,
    OnlineSecurity INT,
    OnlineBackup INT,
    DeviceProtection INT,
    TechSupport INT,
    StreamingTV INT,
    StreamingMovies INT,
    Contract INT,
    PaperlessBilling INT,
    PaymentMethod INT,
    MonthlyCharges FLOAT,
    TotalCharges FLOAT,
    Churn INT,
    TenureGroup INT,
    ChargeRatio FLOAT
);

-- Decoded table (for Power BI)
CREATE OR REPLACE TABLE churn_schema.churn_decoded_for_powerbi (
    gender VARCHAR(10),
    SeniorCitizen VARCHAR(10),
    Partner VARCHAR(10),
    Dependents VARCHAR(10),
    tenure INT,
    PhoneService VARCHAR(10),
    MultipleLines VARCHAR(25),
    InternetService VARCHAR(25),
    OnlineSecurity VARCHAR(30),
    OnlineBackup VARCHAR(30),
    DeviceProtection VARCHAR(30),
    TechSupport VARCHAR(30),
    StreamingTV VARCHAR(30),
    StreamingMovies VARCHAR(30),
    Contract VARCHAR(25),
    PaperlessBilling VARCHAR(10),
    PaymentMethod VARCHAR(35),
    MonthlyCharges FLOAT,
    TotalCharges FLOAT,
    Churn VARCHAR(10),
    TenureGroup VARCHAR(20),
    ChargeRatio FLOAT,
    ChurnRisk VARCHAR(20),
    CustomerSegment VARCHAR(30),
    LoadDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

#### 3.3 Create File Format and Stage

```sql
-- File format
CREATE OR REPLACE FILE FORMAT file_format_schema.churn_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL');

-- External stage pointing to S3
CREATE OR REPLACE STAGE external_stage_schema.churn_ext_stage
    URL = 's3://second-customer-churn-transformed/snowflake/'
    CREDENTIALS = (
        AWS_KEY_ID = 'YOUR_ACCESS_KEY',
        AWS_SECRET_KEY = 'YOUR_SECRET_KEY'
    )
    FILE_FORMAT = file_format_schema.churn_csv_format;

-- Test stage
LIST @external_stage_schema.churn_ext_stage;
```

#### 3.4 Create Snowpipe

```sql
-- Create Snowpipe for auto-ingestion
CREATE OR REPLACE PIPE snowpipe_schema.churn_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO churn_schema.churn_transformed_encoded
FROM @external_stage_schema.churn_ext_stage
FILE_FORMAT = (FORMAT_NAME = file_format_schema.churn_csv_format)
PATTERN = '.*_transformed\\.csv'
ON_ERROR = 'CONTINUE';

-- Get SQS ARN for S3 event notification
DESC PIPE snowpipe_schema.churn_pipe;
-- Copy the notification_channel value
```

#### 3.5 Configure S3 Event Notification

1. Go to S3 → `second-customer-churn-transformed`
2. **Properties** → **Event notifications** → **Create**
3. Configure:
   - Name: `snowpipe-churn-notification`
   - Event types: All object create events
   - Prefix: `snowflake/`
   - Suffix: `_transformed.csv`
   - Destination: SQS queue
   - SQS ARN: Paste from DESC PIPE output

#### 3.6 Create Decoded View

```sql
-- See snowflake/create_decoded_view.sql for full view definition
CREATE OR REPLACE VIEW churn_schema.vw_churn_decoded AS
SELECT 
    CASE gender WHEN 1 THEN 'Male' WHEN 0 THEN 'Female' END AS gender,
    CASE SeniorCitizen WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' END AS SeniorCitizen,
    -- ... (full decoding logic in SQL file)
FROM churn_schema.churn_transformed_encoded;
```

---

### Step 4: Power BI Dashboard Setup

#### 4.1 Connect to Snowflake

1. Open **Power BI Desktop**
2. **Get Data** → **Snowflake**
3. Enter connection details:
   - Server: `your-account.snowflakecomputing.com`
   - Warehouse: `COMPUTE_WH`
4. Authenticate with Snowflake credentials
5. Navigate to: `telecom_churn_db` → `churn_schema` → `vw_churn_decoded`
6. Click **Load**

#### 4.2 Create Visualizations

Dashboard includes 9 key visualizations:
1. **Churn by Contract Type** (Stacked Bar Chart)
2. **Average Monthly Charges by Churn** (Column Chart)
3. **Churn by Tenure Group** (Stacked Bar Chart)
4. **Churn by Gender** (Pie Chart)
5. **Customer Segmentation** (Donut Chart)
6. **Churn Risk Distribution** (Donut Chart)
7. **Churn by Internet Service** (Stacked Bar Chart)
8. **Churn by Payment Method** (100% Stacked Bar Chart)
9. **Churn by Senior Citizen** (Stacked Bar Chart)

---

### Step 5: Test End-to-End Pipeline

```bash
# Upload test CSV
aws s3 cp data/CustomerChurn.csv s3://first-customer-churn-raw/CustomerChurn.csv

# Monitor progress:
# 1. Check Lambda 1 CloudWatch logs (1-2 minutes)
# 2. Check S3 transformed bucket for files in ml/ and snowflake/ folders
# 3. Check Lambda 2 CloudWatch logs (ML training, ~1 minute)
# 4. Check S3 ML bucket for model files
# 5. Check Snowflake table row count (2-3 minutes for Snowpipe)
# 6. Trigger Airflow DAG manually to validate
# 7. Refresh Power BI to see new data
```

---

## 🔄 Pipeline Workflow

### Detailed Step-by-Step Process

#### **Stage 1: Data Ingestion**
```
1. User uploads CustomerChurn.csv to S3 raw bucket
2. S3 generates PUT event
3. Event triggers Lambda 1 automatically
```

#### **Stage 2: Data Transformation (Lambda 1)**
```
4. Lambda 1 reads CSV from S3
5. Data Cleaning:
   - Convert TotalCharges to numeric
   - Fill missing values with MonthlyCharges
   - Remove customerID column
6. Feature Engineering:
   - Create TenureGroup (0-1yr, 1-2yr, 2-4yr, 4+yr)
   - Calculate ChargeRatio = TotalCharges / (tenure + 1)
7. Encoding:
   - Binary: Yes/No → 1/0
   - Categorical: Manual label encoding
8. Save to TWO locations:
   - ml/CustomerChurn_transformed.csv (triggers Lambda 2)
   - snowflake/CustomerChurn_transformed.csv (triggers Snowpipe)
```

#### **Stage 3: ML Model Training (Lambda 2)**
```
9. S3 PUT event in ml/ folder triggers Lambda 2
10. Lambda 2 loads transformed data
11. Split data: 80% train, 20% test (stratified)
12. Train Random Forest:
    - 100 estimators
    - Max depth: 10
    - Balanced class weights
13. Evaluate model:
    - Calculate accuracy, precision, recall, F1-score
    - Generate confusion matrix
    - Identify feature importance
14. Save artifacts to S3 ML bucket:
    - random_forest_churn_model_TIMESTAMP.pkl
    - random_forest_churn_model_latest.pkl
    - model_metrics_TIMESTAMP.json
    - model_metrics_latest.json
```

#### **Stage 4: Data Warehouse Loading (Snowpipe)**
```
15. S3 PUT event in snowflake/ folder triggers SQS notification
16. Snowpipe receives notification
17. Snowpipe copies data into churn_transformed_encoded table
18. Decoded view (vw_churn_decoded) automatically updates
19. Power BI can now access human-readable data
```

#### **Stage 5: Orchestration Monitoring (Airflow)**
```
20. Airflow DAG runs (scheduled daily or triggered manually):
    Task 1: Check if raw data exists ✓
    Task 2: Wait for Lambda 1 completion ✓
    Task 3: Wait for Lambda 2 completion ✓
    Task 4: Prepare Snowflake metadata ✓
    Task 5: Generate pipeline summary report ✓
21. All task logs available in Airflow UI
22. Email notifications on failure (configurable)
```

#### **Stage 6: Business Intelligence (Power BI)**

<img width="1530" height="848" alt="image" src="https://github.com/user-attachments/assets/74cba597-0182-480e-a471-a04d58c74c52" />

```
23. Power BI connects to Snowflake view
24. Dashboards refresh (scheduled or on-demand)
25. Users analyze churn insights
26. Business decisions made based on data
```

### ML Model Output

**Model Files in S3**:
- `random_forest_churn_model_latest.pkl` - Trained model (serialized)
- `model_metrics_latest.json` - Performance metrics

**Metrics JSON Structure**:
```json
{
  "timestamp": "2025-11-29T14:13:52",
  "model_type": "Random Forest Classifier",
  "model_parameters": {
    "n_estimators": 100,
    "max_depth": 10,
    "min_samples_split": 10,
    "min_samples_leaf": 5,
    "class_weight": "balanced"
  },
  "data_info": {
    "total_samples": 7043,
    "training_samples": 5634,
    "test_samples": 1409,
    "num_features": 21
  },
  "performance": {
    "accuracy": 0.7743,
    "churn_class": {
      "precision": 0.55,
      "recall": 0.76,
      "f1_score": 0.64
    },
    "no_churn_class": {
      "precision": 0.90,
      "recall": 0.78,
      "f1_score": 0.84
    }
  },
  "feature_importance": [
    {"feature": "Contract", "importance": 0.1643},
    {"feature": "tenure", "importance": 0.1531},
    {"feature": "TotalCharges", "importance": 0.0929}
  ]
}
```

---

## 🤖 ML Model Details

### Algorithm: Random Forest Classifier

**Why Random Forest?**
- ✅ Handles non-linear relationships
- ✅ Robust to overfitting (ensemble method)
- ✅ Provides feature importance
- ✅ Works well with imbalanced data
- ✅ No feature scaling required


### Top 10 Important Features


1. Contract — 16.43%: Contract type is strongest predictor
2. tenure — 15.31%: Customer loyalty matters
3. TotalCharges — 9.29%: Spending history is important
4. MonthlyCharges — 8.87%: Monthly bill amount impacts churn
5. ChargeRatio — 7.52%: Engineered feature adds value
6. InternetService — 6.84%: Service type influences churn
7. PaymentMethod — 5.93%: Payment preference matters
8. TechSupport — 5.21%: Support services reduce churn
9. OnlineSecurity — 4.76%: Security features matter
10. TenureGroup — 4.38%: Tenure grouping helps prediction

---

## 📈 Dashboard Insights



### Power BI Dashboard Features

#### 1️⃣ **Executive Summary Page**
- **Total Customers**: 7,043
- **Churn Rate**: 26.5%
- **Average Monthly Revenue**: $64.76
- **High-Risk Customers**: 1,869 (26.5%)

#### 2️⃣ **Churn Analysis Page**

**Key Findings**:
- 📊 **Contract Type**: Month-to-month contracts have 42% churn rate vs 11% for yearly contracts
- 📊 **Tenure**: Customers with <1 year tenure have 50% churn rate
- 📊 **Internet Service**: Fiber optic customers churn at 41% vs DSL at 19%
- 📊 **Payment Method**: Electronic check users have highest churn (45%)

#### 3️⃣ **Customer Segmentation Page**

**Risk Categories**:
- 🔴 **High Risk**: Month-to-month + <1yr tenure + Electronic check
- 🟡 **Medium Risk**: 1-2yr tenure + Fiber optic
- 🟢 **Low Risk**: 2yr contract + >4yr tenure + Tech support

#### 4️⃣ **Financial Impact Page**

**Revenue Analysis**:
- 💰 **Lost Revenue**: $16.06M/year from churned customers
- 💰 **High-Risk Customer MRR**: $4.2M/month
- 💰 **Retention Opportunity**: $50.4M/year if 100% retention

---

## 🔮 Future Enhancements

### Technical Improvements
- [ ] Implement **real-time streaming** with Kinesis/Kafka
- [ ] Add **model retraining pipeline** (monthly auto-retrain)
- [ ] Integrate **A/B testing framework** for model versions
- [ ] Add **explainable AI** (SHAP values) for predictions
- [ ] Implement **data versioning** with DVC or Delta Lake

### ML Enhancements
- [ ] Try **ensemble models** (XGBoost, LightGBM)
- [ ] Implement **hyperparameter tuning** with Optuna
- [ ] Add **customer lifetime value** (CLV) prediction
- [ ] Create **churn probability scores** (0-100 scale)
- [ ] Build **next-best-action** recommendation system

### Analytics & BI
- [ ] Add **real-time alerting** for high-risk customers
- [ ] Create **customer journey analysis** dashboard
- [ ] Implement **cohort analysis** for retention tracking
- [ ] Add **predictive retention campaigns** automation
- [ ] Build **what-if scenario modeling** tool

### Infrastructure
- [ ] Migrate to **containerized deployment** (Docker/ECS)
- [ ] Add **CI/CD pipeline** with GitHub Actions
- [ ] Implement **infrastructure as code** (Terraform)
- [ ] Add **automated testing** (unit, integration, E2E)
- [ ] Create **disaster recovery** and backup strategies

---
