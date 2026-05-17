import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

// Adding Lambdas
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from 'path'; // Useful for resolving file paths

// Addding EventBridge (for cron jobs)
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

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
      // A. Define the Lambda Function
      const ingestorFn = new lambda.Function(this, `${source}Ingestor`, {
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

      // B. Define the EventBridge Rule (New)
      // This example runs every 12 hours.

      // Explaining the Cron Syntax: cron(0 */6 * * ? *)
      // AWS EventBridge cron expressions have 6 fields: Minutes, Hours, Day-of-month, Month, Day-of-week, Year.
      // - 0: The 0th minute.
      // - */12: Every 12 hours.
      // - *: Every day of the month.
      // - *: Every month.
      // - ?: Any day of the week (required if Day-of-month is specified).
      // - *: Every year.

      const rule = new events.Rule(this, `${source}ScheduleRule`, {
        schedule: events.Schedule.expression('cron(0 */12 * * ? *)'),
        description: `Scheduled trigger for ${source} data ingestion`,
      });

      // C. Add the Lambda as a target for the Rule
      rule.addTarget(new targets.LambdaFunction(ingestorFn));
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