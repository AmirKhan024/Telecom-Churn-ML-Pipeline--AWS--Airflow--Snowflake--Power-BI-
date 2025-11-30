DROP DATABASE IF EXISTS telecom_churn_db;
CREATE DATABASE telecom_churn_db;

CREATE SCHEMA telecom_churn_db.churn_schema;

DROP TABLE IF EXISTS telecom_churn_db.churn_schema.churn_transformed;

CREATE OR REPLACE TABLE telecom_churn_db.churn_schema.churn_transformed (
    gender STRING,
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

-- decode table 

-- Drop the existing table
DROP TABLE IF EXISTS churn_schema.churn_decoded_for_powerbi;

-- Create with correct sizes
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


-------
SELECT * FROM telecom_churn_db.churn_schema.churn_transformed LIMIT 10;


CREATE SCHEMA telecom_churn_db.file_format_schema;

CREATE OR REPLACE FILE FORMAT telecom_churn_db.file_format_schema.churn_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL');

    
CREATE SCHEMA telecom_churn_db.external_stage_schema;

CREATE OR REPLACE STAGE telecom_churn_db.external_stage_schema.churn_ext_stage
    URL = 's3://second-customer-churn-transformed/snowflake/'
    CREDENTIALS = (
        AWS_KEY_ID = 'x',
        AWS_SECRET_KEY = 'y'
    )
    FILE_FORMAT = telecom_churn_db.file_format_schema.churn_csv_format;

    
LIST @telecom_churn_db.external_stage_schema.churn_ext_stage;

DROP PIPE IF EXISTS telecom_churn_db.snowpipe_schema.churn_pipe;
  
CREATE SCHEMA telecom_churn_db.snowpipe_schema;

CREATE OR REPLACE PIPE telecom_churn_db.snowpipe_schema.churn_pipe
    AUTO_INGEST = TRUE
AS
COPY INTO telecom_churn_db.churn_schema.churn_transformed
FROM @telecom_churn_db.external_stage_schema.churn_ext_stage
FILE_FORMAT = (FORMAT_NAME = telecom_churn_db.file_format_schema.churn_csv_format)
PATTERN = '.*_transformed\\.csv'
ON_ERROR = 'CONTINUE';


truncate table telecom_churn_db.churn_schema.churn_transformed



DESC PIPE telecom_churn_db.snowpipe_schema.churn_pipe;

DESC TABLE telecom_churn_db.churn_schema.churn_transformed;


-- Option A: Create a VIEW (automatically updates)
CREATE OR REPLACE VIEW churn_schema.vw_churn_decoded AS
SELECT 
    -- Decode gender
    CASE gender 
        WHEN 1 THEN 'Male' 
        WHEN 0 THEN 'Female' 
        ELSE 'Unknown' 
    END AS gender,
    
    -- Decode SeniorCitizen
    CASE SeniorCitizen 
        WHEN 1 THEN 'Yes' 
        WHEN 0 THEN 'No' 
        ELSE 'Unknown' 
    END AS SeniorCitizen,
    
    -- Decode Partner
    CASE Partner 
        WHEN 1 THEN 'Yes' 
        WHEN 0 THEN 'No' 
        ELSE 'Unknown' 
    END AS Partner,
    
    -- Decode Dependents
    CASE Dependents 
        WHEN 1 THEN 'Yes' 
        WHEN 0 THEN 'No' 
        ELSE 'Unknown' 
    END AS Dependents,
    
    -- Keep tenure as-is
    tenure,
    
    -- Decode PhoneService
    CASE PhoneService 
        WHEN 1 THEN 'Yes' 
        WHEN 0 THEN 'No' 
        ELSE 'Unknown' 
    END AS PhoneService,
    
    -- Decode MultipleLines
    CASE MultipleLines 
        WHEN 2 THEN 'Yes' 
        WHEN 1 THEN 'No' 
        WHEN 0 THEN 'No phone service' 
        ELSE 'Unknown' 
    END AS MultipleLines,
    
    -- Decode InternetService
    CASE InternetService 
        WHEN 0 THEN 'DSL' 
        WHEN 1 THEN 'Fiber optic' 
        WHEN 2 THEN 'No' 
        ELSE 'Unknown' 
    END AS InternetService,
    
    -- Decode OnlineSecurity
    CASE OnlineSecurity 
        WHEN 2 THEN 'Yes' 
        WHEN 1 THEN 'No' 
        WHEN 0 THEN 'No internet service' 
        ELSE 'Unknown' 
    END AS OnlineSecurity,
    
    -- Decode OnlineBackup
    CASE OnlineBackup 
        WHEN 2 THEN 'Yes' 
        WHEN 1 THEN 'No' 
        WHEN 0 THEN 'No internet service' 
        ELSE 'Unknown' 
    END AS OnlineBackup,
    
    -- Decode DeviceProtection
    CASE DeviceProtection 
        WHEN 2 THEN 'Yes' 
        WHEN 1 THEN 'No' 
        WHEN 0 THEN 'No internet service' 
        ELSE 'Unknown' 
    END AS DeviceProtection,
    
    -- Decode TechSupport
    CASE TechSupport 
        WHEN 2 THEN 'Yes' 
        WHEN 1 THEN 'No' 
        WHEN 0 THEN 'No internet service' 
        ELSE 'Unknown' 
    END AS TechSupport,
    
    -- Decode StreamingTV
    CASE StreamingTV 
        WHEN 2 THEN 'Yes' 
        WHEN 1 THEN 'No' 
        WHEN 0 THEN 'No internet service' 
        ELSE 'Unknown' 
    END AS StreamingTV,
    
    -- Decode StreamingMovies
    CASE StreamingMovies 
        WHEN 2 THEN 'Yes' 
        WHEN 1 THEN 'No' 
        WHEN 0 THEN 'No internet service' 
        ELSE 'Unknown' 
    END AS StreamingMovies,
    
    -- Decode Contract
    CASE Contract 
        WHEN 0 THEN 'Month-to-month' 
        WHEN 1 THEN 'One year' 
        WHEN 2 THEN 'Two year' 
        ELSE 'Unknown' 
    END AS Contract,
    
    -- Decode PaperlessBilling
    CASE PaperlessBilling 
        WHEN 1 THEN 'Yes' 
        WHEN 0 THEN 'No' 
        ELSE 'Unknown' 
    END AS PaperlessBilling,
    
    -- Decode PaymentMethod
    CASE PaymentMethod 
        WHEN 0 THEN 'Electronic check' 
        WHEN 1 THEN 'Mailed check' 
        WHEN 2 THEN 'Bank transfer (automatic)' 
        WHEN 3 THEN 'Credit card (automatic)' 
        ELSE 'Unknown' 
    END AS PaymentMethod,
    
    -- Keep numeric fields
    MonthlyCharges,
    TotalCharges,
    
    -- Decode Churn
    CASE Churn 
        WHEN 1 THEN 'Yes' 
        WHEN 0 THEN 'No' 
        ELSE 'Unknown' 
    END AS Churn,
    
    -- Decode TenureGroup
    CASE TenureGroup 
        WHEN 0 THEN '0-1 year' 
        WHEN 1 THEN '1-2 years' 
        WHEN 2 THEN '2-4 years' 
        WHEN 3 THEN '4+ years' 
        ELSE 'Unknown' 
    END AS TenureGroup,
    
    ChargeRatio,
    
    -- Additional calculated fields for better insights
    CASE 
        WHEN Churn = 1 THEN 'Churned'
        WHEN ChargeRatio > 100 THEN 'High Risk'
        WHEN ChargeRatio > 50 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS ChurnRisk,
    
    CASE 
        WHEN tenure <= 12 THEN 'New Customer'
        WHEN tenure <= 24 THEN 'Growing Customer'
        WHEN tenure <= 48 AND Churn = 0 THEN 'Loyal Customer'
        WHEN tenure > 48 AND Churn = 0 THEN 'Very Loyal Customer'
        ELSE 'At-Risk Customer'
    END AS CustomerSegment
    
FROM telecom_churn_db.churn_schema.churn_transformed;

----------

CREATE OR REPLACE PROCEDURE churn_schema.sp_refresh_decoded_table()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Truncate decoded table
    TRUNCATE TABLE churn_schema.churn_decoded_for_powerbi;
    
    -- Insert decoded data
    INSERT INTO churn_schema.churn_decoded_for_powerbi (
        gender, SeniorCitizen, Partner, Dependents, tenure, PhoneService,
        MultipleLines, InternetService, OnlineSecurity, OnlineBackup,
        DeviceProtection, TechSupport, StreamingTV, StreamingMovies,
        Contract, PaperlessBilling, PaymentMethod, MonthlyCharges,
        TotalCharges, Churn, TenureGroup, ChargeRatio, ChurnRisk, CustomerSegment
    )
    SELECT 
        gender, SeniorCitizen, Partner, Dependents, tenure, PhoneService,
        MultipleLines, InternetService, OnlineSecurity, OnlineBackup,
        DeviceProtection, TechSupport, StreamingTV, StreamingMovies,
        Contract, PaperlessBilling, PaymentMethod, MonthlyCharges,
        TotalCharges, Churn, TenureGroup, ChargeRatio, ChurnRisk, CustomerSegment
    FROM churn_schema.vw_churn_decoded;
    
    RETURN 'Decoded table refreshed successfully. Rows inserted: ' || SQLROWCOUNT;
END;
$$;

CALL churn_schema.sp_refresh_decoded_table();
----


-- Check encoded data
SELECT COUNT(*) AS encoded_row_count 
FROM telecom_churn_db.churn_schema.churn_transformed;

-- Check decoded view
SELECT COUNT(*) AS decoded_row_count 
FROM churn_schema.vw_churn_decoded;

-- Check decoded table (after running stored procedure)
SELECT COUNT(*) AS decoded_table_count 
FROM churn_schema.churn_decoded_for_powerbi;

-- Sample decoded data for Power BI
SELECT * FROM churn_schema.vw_churn_decoded LIMIT 20;

-- Sample from decoded table
SELECT * FROM churn_schema.churn_decoded_for_powerbi LIMIT 2;

-- Check churn distribution
SELECT 
    Churn,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM churn_schema.vw_churn_decoded
GROUP BY Churn;

-- Check by contract type
SELECT 
    Contract,
    Churn,
    COUNT(*) AS customer_count
FROM churn_schema.vw_churn_decoded
GROUP BY Contract, Churn
ORDER BY Contract, Churn;

SELECT COUNT(*) FROM telecom_churn_db.churn_schema.churn_transformed;
SELECT * FROM telecom_churn_db.churn_schema.churn_transformed LIMIT 20;


