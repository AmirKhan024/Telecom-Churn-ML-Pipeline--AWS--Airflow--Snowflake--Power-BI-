import json
import boto3
import pandas as pd
from io import StringIO

s3_client = boto3.client('s3')

# Bucket names
RAW_BUCKET = 'first-customer-churn-raw'
TRANSFORMED_BUCKET = 'second-customer-churn-transformed'

def lambda_handler(event, context):
    """
    Lambda 1: Clean and encode customer churn data
    Uses: pandas only
    Saves to two locations: ml/ (for Lambda 2) and snowflake/ (for Snowpipe)
    """
    try:
        # Get file info from S3 event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        print(f"[Lambda 1] Processing: {object_key} from {source_bucket}")
        
        # Wait for object to exist
        waiter = s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=source_bucket, Key=object_key)
        
        # Read CSV from S3
        response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Load into DataFrame
        df = pd.read_csv(StringIO(csv_content))
        print(f"[Lambda 1] Original shape: {df.shape}")
        print(f"[Lambda 1] Original columns: {df.columns.tolist()}")
        
        
        # Handle TotalCharges - convert to numeric
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
        
        # Fill missing TotalCharges with MonthlyCharges
        df['TotalCharges'].fillna(df['MonthlyCharges'], inplace=True)
        
        # Remove customerID (not needed for ML)
        df_cleaned = df.drop('customerID', axis=1)
        
        # Convert SeniorCitizen from 0/1 to No/Yes for consistency
        df_cleaned['SeniorCitizen'] = df_cleaned['SeniorCitizen'].map({0: 'No', 1: 'Yes'})
        
        print(f"[Lambda 1] After cleaning: {df_cleaned.shape}")
        
        # Create Tenure Groups
        def categorize_tenure(tenure):
            if tenure <= 12:
                return '0-1_year'
            elif tenure <= 24:
                return '1-2_years'
            elif tenure <= 48:
                return '2-4_years'
            else:
                return '4+_years'
        
        df_cleaned['TenureGroup'] = df_cleaned['tenure'].apply(categorize_tenure)
        
        # Create Charge Ratio feature
        df_cleaned['ChargeRatio'] = df_cleaned['TotalCharges'] / (df_cleaned['tenure'] + 1)
        
        print(f"[Lambda 1] After feature engineering: {df_cleaned.shape}")

        
        # Binary Yes/No columns
        binary_cols = ['Partner', 'Dependents', 'PhoneService', 'PaperlessBilling', 'Churn']
        
        for col in binary_cols:
            df_cleaned[col] = df_cleaned[col].map({'Yes': 1, 'No': 0})
        
        # Gender encoding
        df_cleaned['gender'] = df_cleaned['gender'].map({'Male': 1, 'Female': 0})
        
        # SeniorCitizen encoding
        df_cleaned['SeniorCitizen'] = df_cleaned['SeniorCitizen'].map({'Yes': 1, 'No': 0})
        
        # MultipleLines encoding (3 categories)
        df_cleaned['MultipleLines'] = df_cleaned['MultipleLines'].map({
            'Yes': 2,
            'No': 1,
            'No phone service': 0
        })
        
        # Internet service related columns (3 categories each)
        internet_cols = ['OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 
                         'TechSupport', 'StreamingTV', 'StreamingMovies']
        
        for col in internet_cols:
            df_cleaned[col] = df_cleaned[col].map({
                'Yes': 2,
                'No': 1,
                'No internet service': 0
            })
        
        print(f"[Lambda 1] After binary encoding")
        

        
        # Manual encoding for categorical columns (to avoid sklearn dependency)
        
        # InternetService: DSL, Fiber optic, No
        internet_service_map = {
            'DSL': 0,
            'Fiber optic': 1,
            'No': 2
        }
        df_cleaned['InternetService'] = df_cleaned['InternetService'].map(internet_service_map)
        
        # Contract: Month-to-month, One year, Two year
        contract_map = {
            'Month-to-month': 0,
            'One year': 1,
            'Two year': 2
        }
        df_cleaned['Contract'] = df_cleaned['Contract'].map(contract_map)
        
        # PaymentMethod: Electronic check, Mailed check, Bank transfer, Credit card
        payment_map = {
            'Electronic check': 0,
            'Mailed check': 1,
            'Bank transfer (automatic)': 2,
            'Credit card (automatic)': 3
        }
        df_cleaned['PaymentMethod'] = df_cleaned['PaymentMethod'].map(payment_map)
        
        # TenureGroup
        tenure_group_map = {
            '0-1_year': 0,
            '1-2_years': 1,
            '2-4_years': 2,
            '4+_years': 3
        }
        df_cleaned['TenureGroup'] = df_cleaned['TenureGroup'].map(tenure_group_map)
        
        print(f"[Lambda 1] After categorical encoding: {df_cleaned.shape}")
        print(f"[Lambda 1] Final columns: {df_cleaned.columns.tolist()}")
        
        # Check for any missing values
        missing_values = df_cleaned.isnull().sum()
        if missing_values.any():
            print(f"[Lambda 1] WARNING - Missing values found:")
            print(missing_values[missing_values > 0])
        

        # Convert DataFrame to CSV (do this once)
        csv_buffer = StringIO()
        df_cleaned.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Create base transformed filename
        base_filename = object_key.replace('.csv', '_transformed.csv')
        
        # ===== SAVE FOR ML TRAINING (Lambda 2) =====
        ml_key = f'ml/{base_filename}'
        s3_client.put_object(
            Bucket=TRANSFORMED_BUCKET,
            Key=ml_key,
            Body=csv_content
        )
        print(f"[Lambda 1] ✅ Saved for ML training: s3://{TRANSFORMED_BUCKET}/{ml_key}")
        
        # ===== SAVE FOR SNOWFLAKE (Snowpipe) =====
        snowflake_key = f'snowflake/{base_filename}'
        s3_client.put_object(
            Bucket=TRANSFORMED_BUCKET,
            Key=snowflake_key,
            Body=csv_content
        )
        print(f"[Lambda 1] ✅ Saved for Snowflake: s3://{TRANSFORMED_BUCKET}/{snowflake_key}")

        
        encoding_info = {
            'binary_mappings': {
                'gender': {'Male': 1, 'Female': 0},
                'SeniorCitizen': {'Yes': 1, 'No': 0},
                'binary_yes_no': {'Yes': 1, 'No': 0},
                'MultipleLines': {'Yes': 2, 'No': 1, 'No phone service': 0},
                'internet_services': {'Yes': 2, 'No': 1, 'No internet service': 0}
            },
            'categorical_mappings': {
                'InternetService': internet_service_map,
                'Contract': contract_map,
                'PaymentMethod': payment_map,
                'TenureGroup': tenure_group_map
            },
            'feature_columns': df_cleaned.columns.tolist(),
            'total_rows': len(df_cleaned),
            'total_features': len(df_cleaned.columns)
        }
        
        # Save mappings as JSON
        s3_client.put_object(
            Bucket=TRANSFORMED_BUCKET,
            Key='encoding_mappings.json',
            Body=json.dumps(encoding_info, indent=4)
        )
        
        print(f"[Lambda 1] ✅ Saved encoding mappings")
        

        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data transformation completed successfully',
                'ml_file': f's3://{TRANSFORMED_BUCKET}/{ml_key}',
                'snowflake_file': f's3://{TRANSFORMED_BUCKET}/{snowflake_key}',
                'rows_processed': len(df_cleaned),
                'features': len(df_cleaned.columns),
                'lambda_function': 'Lambda 1 - Transform'
            })
        }
        
    except Exception as e:
        print(f"[Lambda 1] ❌ ERROR: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'lambda_function': 'Lambda 1 - Transform'
            })
        }