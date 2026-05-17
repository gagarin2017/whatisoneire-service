import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from 'path'; // Useful for resolving file paths

export class InfraCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 1. Define the Kinesis Raw Data Stream
    const rawStream = new kinesis.Stream(this, 'EventsRawStream', {
      streamName: 'events-raw-stream',
      shardCount: 1,
      retentionPeriod: cdk.Duration.hours(24),
    });

    const jarPath = path.join(__dirname, '../../../infra/target/scala-3.3.5/whats-on-eire-infra-assembly-0.1.0-SNAPSHOT.jar');

    const dataSources = ['Ticketmaster', 'Meetup', 'FailteIreland', 'DataGov'];
  
    dataSources.forEach(source => {
      new lambda.Function(this, `${source}Ingestor`, {
        runtime: lambda.Runtime.JAVA_21, // Matches your project setup
        handler: 'what.is.on.eire.LambdaHandler', // The entry point in your Scala code
        code: lambda.Code.fromAsset(jarPath),
        timeout: cdk.Duration.seconds(30),
        memorySize: 512,
       environment: {
         SOURCE_NAME: source,
         STREAM_NAME: rawStream.streamName,
       },
     });
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