#!/usr/bin/env bash
set -e

echo "====================================================="
echo "=== Provisioning LocalStack Infrastructure via CDK ==="
echo "====================================================="
echo ""

# Common Docker execution shortcut with credentials embedded for manual checks
DOCKER_CMD="docker exec -e AWS_ACCESS_KEY_ID=mock_key -e AWS_SECRET_ACCESS_KEY=mock_secret -e AWS_DEFAULT_REGION=eu-west-1 woe_localstack"

# Ensure we are executing from the root directory of the project
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Keep the CDK CLI and LocalStack pointed at the same mock account/region.
export AWS_ACCESS_KEY_ID="mock_key"
export AWS_SECRET_ACCESS_KEY="mock_secret"
export AWS_DEFAULT_REGION="eu-west-1"
export AWS_REGION="eu-west-1"
export CDK_DEFAULT_ACCOUNT="000000000000"
export CDK_DEFAULT_REGION="eu-west-1"

cleanup_legacy_resource() {
    local service="$1"
    local name="$2"

    case "$service" in
        kinesis)
            if $DOCKER_CMD aws kinesis describe-stream --stream-name "$name" --endpoint-url http://localhost:4566 >/dev/null 2>&1; then
                echo "Found legacy Kinesis stream '$name'. Deleting it so CDK can recreate and manage it..."
                $DOCKER_CMD aws kinesis delete-stream --stream-name "$name" --enforce-consumer-deletion --endpoint-url http://localhost:4566
                until ! $DOCKER_CMD aws kinesis describe-stream --stream-name "$name" --endpoint-url http://localhost:4566 >/dev/null 2>&1; do
                    sleep 2
                done
                echo "Legacy Kinesis stream '$name' removed."
            fi
            ;;
        dynamodb)
            if $DOCKER_CMD aws dynamodb describe-table --table-name "$name" --endpoint-url http://localhost:4566 >/dev/null 2>&1; then
                echo "Found legacy DynamoDB table '$name'. Deleting it so CDK can recreate and manage it..."
                $DOCKER_CMD aws dynamodb delete-table --table-name "$name" --endpoint-url http://localhost:4566 >/dev/null
                until ! $DOCKER_CMD aws dynamodb describe-table --table-name "$name" --endpoint-url http://localhost:4566 >/dev/null 2>&1; do
                    sleep 2
                done
                echo "Legacy DynamoDB table '$name' removed."
            fi
            ;;
    esac
}

cleanup_failed_stack() {
    local stack_name="$1"
    local stack_status

    stack_status=$($DOCKER_CMD aws cloudformation describe-stacks --stack-name "$stack_name" --endpoint-url http://localhost:4566 --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "MISSING")

    case "$stack_status" in
        CREATE_FAILED|ROLLBACK_COMPLETE|ROLLBACK_FAILED|DELETE_FAILED|UPDATE_ROLLBACK_FAILED|UPDATE_ROLLBACK_COMPLETE)
            echo "Found failed CloudFormation stack '$stack_name' in state '$stack_status'. Deleting it before redeploy..."
            $DOCKER_CMD aws cloudformation delete-stack --stack-name "$stack_name" --endpoint-url http://localhost:4566
            until ! $DOCKER_CMD aws cloudformation describe-stacks --stack-name "$stack_name" --endpoint-url http://localhost:4566 >/dev/null 2>&1; do
                sleep 2
            done
            echo "Failed CloudFormation stack '$stack_name' removed."
            ;;
    esac
}

# --------------------------------------------------------------------
# 1. Validate CDK Tooling
# --------------------------------------------------------------------
if ! command -v cdklocal &> /dev/null; then
    echo "❌ Error: 'cdklocal' is not installed."
    echo "Please run: npm install -g aws-cdk-local aws-cdk"
    exit 1
fi

# --------------------------------------------------------------------
# 2. Run AWS CDK Local Deployment
# --------------------------------------------------------------------
echo "Navigating to CDK directory..."
cd "$SCRIPT_DIR/infra-cdk"

echo "Cleaning up legacy manually-provisioned resources..."
cleanup_legacy_resource kinesis "events-raw-stream"
cleanup_legacy_resource dynamodb "irish-events"
cleanup_failed_stack "InfraCdkStack"

echo "Deploying CloudFormation Stack to LocalStack..."
# Clean deployment call—no bootstrap needed anymore!
cdklocal deploy --require-approval never

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
echo "=== All Infrastructure Resources Deployed Successfully! ==="
echo "====================================================="
