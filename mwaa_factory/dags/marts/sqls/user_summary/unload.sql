-- S3 エクスポート用 UNLOAD SQL
UNLOAD ('SELECT * FROM mart_schema.user_summary')
TO 's3://my-analytics-bucket/marts/user_summary/'
IAM_ROLE default
FORMAT AS PARQUET
ALLOWOVERWRITE;
