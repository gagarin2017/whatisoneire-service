#!/usr/bin/env bash
set -e

echo "====================================================="
echo "=== Provisioning LocalStack Infrastructure via Docker ==="
echo "====================================================="
echo ""

# Common Docker execution shortcut with credentials embedded
DOCKER_CMD="docker exec -e AWS_ACCESS_KEY_ID=mock_key -e AWS_SECRET_ACCESS_KEY=mock_secret -e AWS_DEFAULT_REGION=eu-west-1 woe_localstack"

# --------------------------------------------------------------------
# 1. Handle Kinesis Stream (Idempotent)
# --------------------------------------------------------------------
echo "Checking Kinesis stream: events-raw-stream..."

if $DOCKER_CMD aws kinesis describe-stream --stream-name events-raw-stream --endpoint-url http://localhost:4566 >/dev/null 2>&1; then
    echo "-> Stream 'events-raw-stream' already exists. Skipping creation."
else
    echo "-> Creating Kinesis stream: events-raw-stream..."
    $DOCKER_CMD aws kinesis create-stream \
        --stream-name events-raw-stream \
        --shard-count 1 \
        --endpoint-url http://localhost:4566
    echo "-> Stream created successfully."
fi

echo ""

# --------------------------------------------------------------------
# 2. Handle DynamoDB Table (Idempotent)
# --------------------------------------------------------------------
echo "Checking DynamoDB table: irish-events..."

if $DOCKER_CMD aws dynamodb describe-table --table-name irish-events --endpoint-url http://localhost:4566 >/dev/null 2>&1; then
    echo "-> Table 'irish-events' already exists. Skipping creation."
else
    echo "-> Creating DynamoDB table: irish-events..."
    $DOCKER_CMD aws dynamodb create-table \
        --table-name irish-events \
        --attribute-definitions \
            AttributeName=county_id,AttributeType=S \
            AttributeName=event_date_id,AttributeType=S \
        --key-schema \
            AttributeName=county_id,KeyType=HASH \
            AttributeName=event_date_id,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST \
        --endpoint-url http://localhost:4566
    echo "-> Table created successfully."
fi

# --------------------------------------------------------------------
# 3. Automated Sanity Checks
# --------------------------------------------------------------------
echo ""
echo "====================================================="
echo "===             RUNNING SANITY CHECKS             ==="
echo "====================================================="
echo ""

echo "[Sanity Check 1/2] Verifying Kinesis Stream..."
STREAM_STATUS=$($DOCKER_CMD aws kinesis describe-stream --stream-name events-raw-stream --endpoint-url http://localhost:4566 --query "StreamDescription.StreamStatus" --output text 2>/dev/null || echo "FAILED")

if [ "$STREAM_STATUS" = "ACTIVE" ]; then
    echo "✅ Kinesis Stream 'events-raw-stream' is online and ACTIVE."
else
    echo "❌ Kinesis Stream check failed! Current status: $STREAM_STATUS"
fi

echo ""

echo "[Sanity Check 2/2] Verifying DynamoDB Table..."
TABLE_STATUS=$($DOCKER_CMD aws dynamodb describe-table --table-name irish-events --endpoint-url http://localhost:4566 --query "Table.TableStatus" --output text 2>/dev/null || echo "FAILED")

if [ "$TABLE_STATUS" = "ACTIVE" ]; then
    echo "✅ DynamoDB Table 'irish-events' is online and ACTIVE."
else
    echo "❌ DynamoDB Table check failed! Current status: $TABLE_STATUS"
fi

echo ""
echo "====================================================="
echo "=== All Mock Infrastructure Resources Are Ready! ==="
echo "====================================================="