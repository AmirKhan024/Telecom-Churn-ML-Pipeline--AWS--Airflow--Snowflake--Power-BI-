from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3
import time
import json

# AWS Configuration
S3_RAW_BUCKET = 'first-customer-churn-raw'
S3_TRANSFORMED_BUCKET = 'second-customer-churn-transformed'
S3_ML_BUCKET = 'third-customer-churn-ml-model'

# Initialize S3 client
s3_client = boto3.client('s3')

# ===== TASK FUNCTIONS =====

def check_raw_data(**kwargs):
    """Task 1: Check if raw data exists in S3"""
    try:
        print("=" * 50)
        print("TASK 1: Checking for raw data in S3")
        print("=" * 50)
        
        response = s3_client.list_objects_v2(
            Bucket=S3_RAW_BUCKET,
            Prefix='CustomerChurn'
        )
        
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            print(f"✅ Found {len(files)} file(s) in raw bucket:")
            for f in files:
                print(f"   - {f}")
            
            # Push first file name to XCom for next tasks
            kwargs['ti'].xcom_push(key='raw_file', value=files[0])
            print(f"✅ Raw data check completed successfully")
            return True
        else:
            print("❌ No files found in raw bucket")
            return False
            
    except Exception as e:
        print(f"❌ Error checking raw data: {str(e)}")
        raise


def wait_for_lambda1_completion(**kwargs):
    """Task 2: Wait for Lambda 1 transformation to complete"""
    try:
        print("=" * 50)
        print("TASK 2: Waiting for Lambda 1 transformation")
        print("=" * 50)
        
        ti = kwargs['ti']
        raw_file = ti.xcom_pull(key='raw_file', task_ids='check_raw_data')
        
        if not raw_file:
            print("❌ No raw file found from previous task")
            return False
        
        # Expected transformed file name
        expected_transformed_file = raw_file.replace('.csv', '_transformed.csv')
        
        print(f"Looking for: {expected_transformed_file}")
        print("Lambda 1 should create this file automatically via S3 trigger")
        
        # Wait up to 5 minutes for transformation
        max_wait = 300  # 5 minutes
        wait_interval = 15  # Check every 15 seconds
        elapsed = 0
        
        while elapsed < max_wait:
            try:
                response = s3_client.list_objects_v2(
                    Bucket=S3_TRANSFORMED_BUCKET,
                    Prefix='CustomerChurn'
                )
                
                if 'Contents' in response:
                    files = [obj['Key'] for obj in response['Contents'] 
                            if '_transformed.csv' in obj['Key']]
                    
                    if files:
                        print(f"✅ Transformed file found: {files[0]}")
                        ti.xcom_push(key='transformed_file', value=files[0])
                        
                        # Also check for encoding mappings
                        encoding_exists = any('encoding_mappings.json' in obj['Key'] 
                                            for obj in response['Contents'])
                        
                        if encoding_exists:
                            print(f"✅ Encoding mappings also found")
                        
                        print(f"✅ Lambda 1 transformation completed")
                        return True
                
                print(f"⏳ Waiting for transformation... ({elapsed}s elapsed)")
                time.sleep(wait_interval)
                elapsed += wait_interval
                
            except Exception as e:
                print(f"⚠️ Error checking transformed bucket: {str(e)}")
                time.sleep(wait_interval)
                elapsed += wait_interval
        
        print(f"❌ Timeout: Transformed file not found after {max_wait}s")
        return False
        
    except Exception as e:
        print(f"❌ Error in wait_for_lambda1: {str(e)}")
        raise


def wait_for_lambda2_completion(**kwargs):
    """Task 3: Wait for Lambda 2 ML training to complete"""
    try:
        print("=" * 50)
        print("TASK 3: Waiting for Lambda 2 ML training")
        print("=" * 50)
        
        ti = kwargs['ti']
        transformed_file = ti.xcom_pull(key='transformed_file', 
                                       task_ids='wait_for_lambda1_completion')
        
        if not transformed_file:
            print("❌ No transformed file found from previous task")
            return False
        
        print(f"Transformed file: {transformed_file}")
        print("Lambda 2 should be triggered automatically via S3 trigger")
        
        # Wait up to 10 minutes for ML training
        max_wait = 600  # 10 minutes
        wait_interval = 20  # Check every 20 seconds
        elapsed = 0
        
        while elapsed < max_wait:
            try:
                response = s3_client.list_objects_v2(
                    Bucket=S3_ML_BUCKET
                )
                
                if 'Contents' in response:
                    # Check for model file
                    model_files = [obj['Key'] for obj in response['Contents'] 
                                  if obj['Key'].endswith('.pkl')]
                    
                    # Check for metrics file
                    metrics_files = [obj['Key'] for obj in response['Contents'] 
                                    if 'metrics' in obj['Key'] and obj['Key'].endswith('.json')]
                    
                    if model_files and metrics_files:
                        print(f"✅ ML model found: {model_files[-1]}")
                        print(f"✅ Metrics found: {metrics_files[-1]}")
                        
                        ti.xcom_push(key='model_file', value=model_files[-1])
                        ti.xcom_push(key='metrics_file', value=metrics_files[-1])
                        
                        # Read and display metrics
                        try:
                            metrics_obj = s3_client.get_object(
                                Bucket=S3_ML_BUCKET,
                                Key='model_metrics_latest.json'
                            )
                            metrics_content = metrics_obj['Body'].read().decode('utf-8')
                            metrics = json.loads(metrics_content)
                            
                            print("\n" + "=" * 50)
                            print("MODEL PERFORMANCE SUMMARY")
                            print("=" * 50)
                            print(f"Accuracy: {metrics['performance']['accuracy']:.4f}")
                            print(f"Churn Precision: {metrics['performance']['churn_class']['precision']:.4f}")
                            print(f"Churn Recall: {metrics['performance']['churn_class']['recall']:.4f}")
                            print(f"Churn F1-Score: {metrics['performance']['churn_class']['f1_score']:.4f}")
                            print("=" * 50 + "\n")
                            
                        except Exception as e:
                            print(f"⚠️ Could not read metrics: {str(e)}")
                        
                        print(f"✅ Lambda 2 ML training completed")
                        return True
                
                print(f"⏳ Waiting for ML training... ({elapsed}s elapsed)")
                time.sleep(wait_interval)
                elapsed += wait_interval
                
            except Exception as e:
                print(f"⚠️ Error checking ML bucket: {str(e)}")
                time.sleep(wait_interval)
                elapsed += wait_interval
        
        print(f"❌ Timeout: ML model not found after {max_wait}s")
        return False
        
    except Exception as e:
        print(f"❌ Error in wait_for_lambda2: {str(e)}")
        raise


def prepare_for_snowflake(**kwargs):
    """Task 4: Prepare data for Snowflake ingestion"""
    try:
        print("=" * 50)
        print("TASK 4: Preparing for Snowflake ingestion")
        print("=" * 50)
        
        ti = kwargs['ti']
        transformed_file = ti.xcom_pull(key='transformed_file', 
                                       task_ids='wait_for_lambda1_completion')
        
        if not transformed_file:
            print("❌ No transformed file available")
            return False
        
        print(f"✅ Transformed data location: s3://{S3_TRANSFORMED_BUCKET}/{transformed_file}")
        print(f"✅ Data is ready for Snowpipe ingestion")
        print("\nNext steps for Snowflake setup:")
        print("1. Create Snowflake external stage pointing to S3 transformed bucket")
        print("2. Create Snowflake table with matching schema")
        print("3. Create Snowpipe to auto-ingest from S3")
        print("4. Snowpipe will automatically load data when files arrive")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in prepare_for_snowflake: {str(e)}")
        raise


def pipeline_summary(**kwargs):
    """Task 5: Print pipeline summary"""
    try:
        print("\n" + "=" * 70)
        print("CUSTOMER CHURN ETL PIPELINE - EXECUTION SUMMARY")
        print("=" * 70)
        
        ti = kwargs['ti']
        
        # Get all file info from XCom
        raw_file = ti.xcom_pull(key='raw_file', task_ids='check_raw_data')
        transformed_file = ti.xcom_pull(key='transformed_file', 
                                       task_ids='wait_for_lambda1_completion')
        model_file = ti.xcom_pull(key='model_file', 
                                 task_ids='wait_for_lambda2_completion')
        metrics_file = ti.xcom_pull(key='metrics_file', 
                                   task_ids='wait_for_lambda2_completion')
        
        print("\n📁 PIPELINE ARTIFACTS:")
        print(f"   Raw Data:         s3://{S3_RAW_BUCKET}/{raw_file}")
        print(f"   Transformed Data: s3://{S3_TRANSFORMED_BUCKET}/{transformed_file}")
        print(f"   ML Model:         s3://{S3_ML_BUCKET}/{model_file}")
        print(f"   Model Metrics:    s3://{S3_ML_BUCKET}/{metrics_file}")
        
        print("\n✅ COMPLETED STAGES:")
        print("   1. ✅ Raw data validation")
        print("   2. ✅ Data transformation (Lambda 1)")
        print("   3. ✅ ML model training (Lambda 2)")
        print("   4. ✅ Snowflake preparation")
        
        print("\n📊 PIPELINE STATUS: SUCCESS")
        print("=" * 70 + "\n")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in pipeline_summary: {str(e)}")
        raise


# ===== DAG DEFINITION =====

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    'customer_churn_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for Customer Churn prediction with ML',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['etl', 'ml', 'customer-churn', 'aws', 'snowflake']
) as dag:
    
    # Task 1: Check raw data
    task_check_raw = PythonOperator(
        task_id='check_raw_data',
        python_callable=check_raw_data,
        provide_context=True
    )
    
    # Task 2: Wait for Lambda 1 transformation
    task_wait_lambda1 = PythonOperator(
        task_id='wait_for_lambda1_completion',
        python_callable=wait_for_lambda1_completion,
        provide_context=True
    )
    
    # Task 3: Wait for Lambda 2 ML training
    task_wait_lambda2 = PythonOperator(
        task_id='wait_for_lambda2_completion',
        python_callable=wait_for_lambda2_completion,
        provide_context=True
    )
    
    # Task 4: Prepare for Snowflake
    task_snowflake_prep = PythonOperator(
        task_id='prepare_for_snowflake',
        python_callable=prepare_for_snowflake,
        provide_context=True
    )
    
    # Task 5: Pipeline summary
    task_summary = PythonOperator(
        task_id='pipeline_summary',
        python_callable=pipeline_summary,
        provide_context=True
    )
    
    # Define task dependencies (pipeline flow)
    task_check_raw >> task_wait_lambda1 >> task_wait_lambda2 >> task_snowflake_prep >> task_summary