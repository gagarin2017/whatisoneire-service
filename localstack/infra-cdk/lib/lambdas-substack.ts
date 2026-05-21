import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from 'path';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';

export interface LambdasSubStackProps extends cdk.NestedStackProps {
  readonly rawStream: kinesis.IStream;
}

/**
 * LambdasSubStack provisions ingestion Lambda functions and EventBridge schedules for various data sources.
 */
export class LambdasSubStack extends cdk.NestedStack {
  constructor(scope: Construct, id: string, props: LambdasSubStackProps) {
    super(scope, id, props);

    // Locate the Scala application JAR built via sbt assembly
    const jarPath = path.join(__dirname, '../../../infra/target/scala-3.3.5/whats-on-eire-infra-assembly-0.1.0-SNAPSHOT.jar');

    // Supported ingestion source providers
    const dataSources = ['Ticketmaster', 'Meetup', 'FailteIreland', 'DataGov'];

    dataSources.forEach(source => {
      // A. Define the Lambda Function
      // This Lambda runs the Scala handler to fetch and ingest data from the external source
      const ingestorFn = new lambda.Function(this, `${source}Ingestor`, {
        runtime: lambda.Runtime.JAVA_21, // Matches your project setup (Java 21 for modern Scala support)
        handler: 'what.is.on.eire.LambdaHandler', // The entry point in your Scala code
        code: lambda.Code.fromAsset(jarPath),
        timeout: cdk.Duration.seconds(30), // Allow up to 30s to fetch/process remote API data
        memorySize: 512, // JVM runtime requires a reasonable memory footprint
        environment: {
          SOURCE_NAME: source, // Identifies the ingestion source
          STREAM_NAME: props.rawStream.streamName, // Target Kinesis stream to publish raw events
        },
      });

      // B. Define the EventBridge Rule (New)
      // This example runs every 12 hours.

      // Explaining the Cron Syntax: cron(0 */12 * * ? *)
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

      // Output values for easier terminal verification and debugging
      new cdk.CfnOutput(this, `${source}LambdaName`, {
        value: ingestorFn.functionName,
        description: `The physical name of the ${source} Ingestor Lambda function`,
      });

      new cdk.CfnOutput(this, `${source}ScheduleRuleName`, {
        value: rule.ruleName,
        description: `The physical name of the EventBridge rule for ${source}`,
      });
    });
  }
}
