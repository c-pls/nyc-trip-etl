COPY INTO {dest_table_name}(
    VENDOR_KEY,
    LPEP_PICKUP_DATE_KEY,
    LPEP_DROPOFF_DATE_KEY,
    LPEP_PICKUP_DATETIME,
    LPEP_DROPOFF_DATETIME,
    RATE_CODE_KEY,
    PICK_UP_LOCATION_KEY,
    DROP_OFF_LOCATION_KEY,
    PAYMENT_TYPE_KEY,
    TRIP_TYPE_KEY,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    STORE_AND_FWD_FLAG,
    FARE_AMOUNT,
    EXTRA,
    EHAIL_FEE,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    IMPROVEMENT_SURCHARGE,
    TOTAL_AMOUNT
)
FROM (
    SELECT
    $1:VENDOR_KEY::NUMBER(38,0) AS VENDOR_KEY,
    $1:LPEP_PICKUP_DATE_KEY::NUMBER(38,0) AS LPEP_PICKUP_DATE_KEY,
    $1:LPEP_DROPOFF_DATE_KEY::NUMBER(38,0) AS LPEP_DROPOFF_DATE_KEY,
    TO_TIMESTAMP_NTZ($1:LPEP_PICKUP_DATETIME::VARCHAR) AS LPEP_PICKUP_DATETIME,
    TO_TIMESTAMP_NTZ($1:LPEP_DROPOFF_DATETIME::VARCHAR) AS LPEP_DROPOFF_DATETIME,
    $1:RATE_CODE_KEY::NUMBER(38,0) AS RATE_CODE_KEY,
    $1:PICK_UP_LOCATION_KEY::NUMBER(38,0) AS PICK_UP_LOCATION_KEY,
    $1:DROP_OFF_LOCATION_KEY::NUMBER(38,0) AS DROP_OFF_LOCATION_KEY,
    $1:PAYMENT_TYPE_KEY::NUMBER(38,0) AS PAYMENT_TYPE_KEY,
    $1:TRIP_TYPE_KEY::NUMBER(38,0) AS TRIP_TYPE_KEY,
    $1:PASSENGER_COUNT::NUMBER(38,0) AS PASSENGER_COUNT,
    $1:TRIP_DISTANCE::FLOAT AS TRIP_DISTANCE,
    $1:STORE_AND_FWD_FLAG::VARCHAR(16777216) AS STORE_AND_FWD_FLAG,
    $1:FARE_AMOUNT::FLOAT AS FARE_AMOUNT,
    $1:EXTRA::FLOAT AS EXTRA,
    $1:EHAIL_FEE::NUMBER(38,0) AS EHAIL_FEE,
    $1:MTA_TAX::FLOAT AS MTA_TAX,
    $1:TIP_AMOUNT::FLOAT AS TIP_AMOUNT,
    $1:TOLLS_AMOUNT::FLOAT AS TOLLS_AMOUNT,
    $1:IMPROVEMENT_SURCHARGE::FLOAT AS IMPROVEMENT_SURCHARGE,
    $1:TOTAL_AMOUNT::FLOAT AS TOTAL_AMOUNT

    FROM @{stage_name}

) PATTERN = '.*parquet.*';