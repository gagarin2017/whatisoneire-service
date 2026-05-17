@echo off
cls
echo =====================================================
echo === Provisioning LocalStack Infrastructure via Docker ===
echo =====================================================
echo.

:: 1. Create Kinesis Stream inside the container directly, passing inline mock credentials
echo Creating Kinesis stream: events-raw-stream...
docker exec -e AWS_ACCESS_KEY_ID=mock_key -e AWS_SECRET_ACCESS_KEY=mock_secret -e AWS_DEFAULT_REGION=eu-west-1 -i woe_localstack aws kinesis create-stream --stream-name events-raw-stream --shard-count 1 --endpoint-url http://localhost:4566

echo.
:: 2. Create DynamoDB Table inside the container directly, passing inline mock credentials
echo Creating DynamoDB table: irish-events...
docker exec -e AWS_ACCESS_KEY_ID=mock_key -e AWS_SECRET_ACCESS_KEY=mock_secret -e AWS_DEFAULT_REGION=eu-west-1 -i woe_localstack aws dynamodb create-table ^
    --table-name irish-events ^
    --attribute-definitions ^
        AttributeName=county_id,AttributeType=S ^
        AttributeName=event_date_id,AttributeType=S ^
    --key-schema ^
        AttributeName=county_id,KeyType=HASH ^
        AttributeName=event_date_id,KeyType=RANGE ^
    --billing-mode PAY_PER_REQUEST ^
    --endpoint-url http://localhost:4566

echo.
echo =====================================================
echo === All Mock Infrastructure Resources Are Ready! ===
echo =====================================================
pause