import json
import boto3
import csv
from io import StringIO, BytesIO
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import pickle
from datetime import datetime
import numpy as np

s3_client = boto3.client('s3')

# Bucket names
TRANSFORMED_BUCKET = 'second-customer-churn-transformed'
ML_MODEL_BUCKET = 'third-customer-churn-ml-model'

def lambda_handler(event, context):
    """
    Lambda 2: Train Random Forest ML model
    Uses: sklearn, numpy (NO pandas)
    """
    try:
        # Get file info from S3 event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        print(f"[Lambda 2] Processing: {object_key} from {source_bucket}")
        
        # Only process transformed CSV files
        if not object_key.endswith('_transformed.csv'):
            print(f"[Lambda 2] Skipping non-transformed file: {object_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Skipped non-transformed file'})
            }
        
        # Wait for object to exist
        waiter = s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=source_bucket, Key=object_key)

        
        response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Parse CSV using built-in csv module
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        # Read all rows into a list
        data_rows = list(csv_reader)
        
        print(f"[Lambda 2] Loaded {len(data_rows)} rows")
        
        # Get column names (features)
        if data_rows:
            all_columns = list(data_rows[0].keys())
            print(f"[Lambda 2] Columns: {all_columns}")
        else:
            raise ValueError("No data found in CSV")

        
        # Separate features and target
        # Churn is the target column
        feature_columns = [col for col in all_columns if col != 'Churn']
        
        # Extract features (X) and target (y)
        X_list = []
        y_list = []
        
        for row in data_rows:
            # Extract features
            features = [float(row[col]) for col in feature_columns]
            X_list.append(features)
            
            # Extract target
            y_list.append(float(row['Churn']))
        
        # Convert to numpy arrays
        X = np.array(X_list)
        y = np.array(y_list)
        
        print(f"[Lambda 2] Features shape: {X.shape}")
        print(f"[Lambda 2] Target shape: {y.shape}")
        print(f"[Lambda 2] Target distribution:")
        print(f"  - Churn=0 (No): {(y==0).sum()}")
        print(f"  - Churn=1 (Yes): {(y==1).sum()}")
        
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, 
            test_size=0.2, 
            random_state=42, 
            stratify=y
        )
        
        print(f"[Lambda 2] Training set: {X_train.shape}")
        print(f"[Lambda 2] Test set: {X_test.shape}")
        
        
        print(f"[Lambda 2] Training Random Forest model...")
        
        rf_model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=1,  # Changed from -1 to avoid permission warning
            class_weight='balanced'
        )
        
        # Train the model
        rf_model.fit(X_train, y_train)
        
        print(f"[Lambda 2] ✅ Model training completed")
        
        
        # Predictions
        y_pred = rf_model.predict(X_test)
        
        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        print(f"[Lambda 2] Model Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
        
        # Classification report
        class_report = classification_report(y_test, y_pred, output_dict=True)
        print(f"[Lambda 2] Classification Report:")
        print(classification_report(y_test, y_pred))
        
        # Confusion matrix
        conf_matrix = confusion_matrix(y_test, y_pred)
        print(f"[Lambda 2] Confusion Matrix:")
        print(conf_matrix)
        
        # Feature importance
        feature_importance_list = []
        for i, importance in enumerate(rf_model.feature_importances_):
            feature_importance_list.append({
                'feature': feature_columns[i],
                'importance': float(importance)
            })
        
        # Sort by importance
        feature_importance_list.sort(key=lambda x: x['importance'], reverse=True)
        
        print(f"[Lambda 2] Top 10 Important Features:")
        for item in feature_importance_list[:10]:
            print(f"  {item['feature']}: {item['importance']:.4f}")

        
        # Save model to /tmp
        model_filename = '/tmp/random_forest_churn_model.pkl'
        with open(model_filename, 'wb') as f:
            pickle.dump(rf_model, f)
        
        # Upload with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        model_key = f'random_forest_churn_model_{timestamp}.pkl'
        
        s3_client.upload_file(
            model_filename,
            ML_MODEL_BUCKET,
            model_key
        )
        
        print(f"[Lambda 2] ✅ Model saved: s3://{ML_MODEL_BUCKET}/{model_key}")
        
        # Save as latest version
        s3_client.upload_file(
            model_filename,
            ML_MODEL_BUCKET,
            'random_forest_churn_model_latest.pkl'
        )
        
        print(f"[Lambda 2] ✅ Latest model updated")
     
        
        # Get the correct key for class labels
        # classification_report uses '0.0', '1.0' or '0', '1' as keys
        churn_class_key = None
        no_churn_class_key = None
        
        # Try different possible key formats
        for key in ['1.0', '1', 1]:
            if str(key) in class_report:
                churn_class_key = str(key)
                break
        
        for key in ['0.0', '0', 0]:
            if str(key) in class_report:
                no_churn_class_key = str(key)
                break
        
        # If still not found, use the keys directly from the report
        if churn_class_key is None or no_churn_class_key is None:
            available_keys = [k for k in class_report.keys() if k not in ['accuracy', 'macro avg', 'weighted avg']]
            if len(available_keys) >= 2:
                no_churn_class_key = available_keys[0]
                churn_class_key = available_keys[1]
        
        print(f"[Lambda 2] Using class keys - No Churn: {no_churn_class_key}, Churn: {churn_class_key}")
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'model_type': 'Random Forest Classifier',
            'model_parameters': {
                'n_estimators': 100,
                'max_depth': 10,
                'min_samples_split': 10,
                'min_samples_leaf': 5,
                'class_weight': 'balanced',
                'n_jobs': 1
            },
            'data_info': {
                'total_samples': len(data_rows),
                'training_samples': len(X_train),
                'test_samples': len(X_test),
                'num_features': len(feature_columns),
                'feature_names': feature_columns
            },
            'performance': {
                'accuracy': float(accuracy),
                'churn_class': {
                    'precision': float(class_report[churn_class_key]['precision']),
                    'recall': float(class_report[churn_class_key]['recall']),
                    'f1_score': float(class_report[churn_class_key]['f1-score']),
                    'support': int(class_report[churn_class_key]['support'])
                },
                'no_churn_class': {
                    'precision': float(class_report[no_churn_class_key]['precision']),
                    'recall': float(class_report[no_churn_class_key]['recall']),
                    'f1_score': float(class_report[no_churn_class_key]['f1-score']),
                    'support': int(class_report[no_churn_class_key]['support'])
                },
                'confusion_matrix': conf_matrix.tolist(),
                'confusion_matrix_labels': {
                    'true_negative': int(conf_matrix[0][0]),
                    'false_positive': int(conf_matrix[0][1]),
                    'false_negative': int(conf_matrix[1][0]),
                    'true_positive': int(conf_matrix[1][1])
                }
            },
            'feature_importance': feature_importance_list[:10],
            'model_location': f's3://{ML_MODEL_BUCKET}/{model_key}'
        }
        
        # Save metrics with timestamp
        metrics_key = f'model_metrics_{timestamp}.json'
        s3_client.put_object(
            Bucket=ML_MODEL_BUCKET,
            Key=metrics_key,
            Body=json.dumps(metrics, indent=4)
        )
        
        print(f"[Lambda 2] ✅ Metrics saved: s3://{ML_MODEL_BUCKET}/{metrics_key}")
        
        # Save latest metrics
        s3_client.put_object(
            Bucket=ML_MODEL_BUCKET,
            Key='model_metrics_latest.json',
            Body=json.dumps(metrics, indent=4)
        )
        
        print(f"[Lambda 2] ✅ Latest metrics saved")
        

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'ML model training completed successfully',
                'model_location': f's3://{ML_MODEL_BUCKET}/{model_key}',
                'accuracy': float(accuracy),
                'churn_precision': float(class_report[churn_class_key]['precision']),
                'churn_recall': float(class_report[churn_class_key]['recall']),
                'churn_f1_score': float(class_report[churn_class_key]['f1-score']),
                'training_samples': len(X_train),
                'test_samples': len(X_test),
                'lambda_function': 'Lambda 2 - ML Training'
            })
        }
        
    except Exception as e:
        print(f"[Lambda 2] ❌ ERROR: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'lambda_function': 'Lambda 2 - ML Training'
            })
        }