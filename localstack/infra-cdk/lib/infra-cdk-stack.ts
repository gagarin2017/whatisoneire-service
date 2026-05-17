import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

export class InfraCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 1. Define the Kinesis Raw Data Stream
    const rawStream = new kinesis.Stream(this, 'EventsRawStream', {
      streamName: 'events-raw-stream',
      shardCount: 1,
      retentionPeriod: cdk.Duration.hours(24),
    });

    // 2. Define the DynamoDB Table
    const eventsTable = new dynamodb.Table(this, 'IrishEventsTable', {
      tableName: 'irish-events',
      partitionKey: { name: 'county_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'event_date_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Safe for local development clearing
    });

    // Output values for easier terminal verification
    new cdk.CfnOutput(this, 'StreamArn', { value: rawStream.streamArn });
    new cdk.CfnOutput(this, 'TableArn', { value: eventsTable.tableArn });
  }
}