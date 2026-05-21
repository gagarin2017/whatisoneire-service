import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

/**
 * DynamoDbSubStack provisions the DynamoDB table for storing normalized Irish event data.
 */
export class DynamoDbSubStack extends cdk.NestedStack {
  public readonly eventsTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props?: cdk.NestedStackProps) {
    super(scope, id, props);

    // 2. Define the DynamoDB Table
    // Partition Key: county_id (allows querying events by Irish county).
    // Sort Key: event_date_id (allows querying/sorting events by date).
    this.eventsTable = new dynamodb.Table(this, 'IrishEventsTable', {
      tableName: 'irish-events',
      partitionKey: { name: 'county_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'event_date_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST, // On-demand scaling (cost-effective for variable traffic)
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Safe for local development clearing
    });

    // Output values for easier terminal verification
    new cdk.CfnOutput(this, 'TableArn', {
      value: this.eventsTable.tableArn,
      description: 'The ARN of the DynamoDB events table',
    });
  }
}
