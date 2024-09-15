-- Minio ignores the region so you can use any region name over here
SET s3_region='us-east-1';
SET s3_url_style='path';
-- Set this endpoint to where minio is at
-- if you are following the blog leave the endpoint as it is
SET s3_endpoint='localhost:9000';
-- this is your minio username
SET s3_access_key_id='minio' ;
-- this is your minio password
SET s3_secret_access_key='minio123';
SET s3_use_ssl=false;
WITH products_create_update_delete AS (
SELECT
    COALESCE(CAST(json->'value'->'after'->'id' AS INT), CAST(json->'value'->'before'->'id' AS INT)) AS id,
        json->'value'->'before' AS before_row_value,
        json->'value'->'after' AS after_row_value,
        CASE
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"c"' THEN 'CREATE'
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"d"' THEN 'DELETE'
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"u"' THEN 'UPDATE'
            WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"r"' THEN 'SNAPSHOT'
            ELSE 'INVALID'
        END AS operation_type,
        CAST(json->'value'->'source'->'lsn' AS BIGINT) AS log_seq_num,
        epoch_ms(CAST(json->'value'->'source'->'ts_ms' AS BIGINT)) AS source_timestamp
    FROM
        read_ndjson_objects('s3://commerce/debezium.commerce.products/*/*/*.json')
    WHERE
        log_seq_num IS NOT NULL
)
SELECT
    id,
    CAST(after_row_value->'name' AS VARCHAR(255)) AS name,
    CAST(after_row_value->'description' AS TEXT) AS description,
    CAST(after_row_value->'price' AS NUMERIC(10, 2)) AS price,
    source_timestamp AS row_valid_start_timestamp,
    -- LEAD(source_timestamp , 1) OVER lead_txn_timestamp AS row_valid_end_timestamp
    CASE 
        WHEN LEAD(source_timestamp, 1) OVER lead_txn_timestamp IS NULL THEN CAST('9999-01-01' AS TIMESTAMP) 
        ELSE LEAD(source_timestamp, 1) OVER lead_txn_timestamp 
    END AS row_valid_expiration_timestamp
FROM products_create_update_delete
WHERE id in (SELECT id FROM products_create_update_delete GROUP BY id HAVING COUNT(*) > 1)
WINDOW lead_txn_timestamp AS (PARTITION BY id ORDER BY log_seq_num )
ORDER BY id, row_valid_start_timestamp
LIMIT
    200;