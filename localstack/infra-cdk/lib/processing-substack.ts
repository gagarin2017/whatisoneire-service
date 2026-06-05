import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as kinesis from "aws-cdk-lib/aws-kinesis";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as path from "path";

export interface ProcessingSubStackProps extends cdk.NestedStackProps {
  readonly rawStream: kinesis.IStream;
  readonly eventsTable: dynamodb.ITable;
}

export class ProcessingSubStack extends cdk.NestedStack {
  constructor(scope: Construct, id: string, props: ProcessingSubStackProps) {
    super(scope, id, props);

    const jarPath = path.join(
      __dirname,
      "../../../infra/target/scala-3.3.5/whats-on-eire-infra-assembly-0.1.0-SNAPSHOT.jar",
    );

    // Single Processing Lambda — reads from the shared Kinesis stream
    // and persists events to DynamoDB. Unlike the ingestion Lambdas,
    // there is only one — it handles all data sources.
    const processorFn = new lambda.Function(this, "EventProcessor", {
      runtime: lambda.Runtime.JAVA_21,
      handler: "what.is.on.eire.ProcessingLambdaHandler",
      code: lambda.Code.fromAsset(jarPath),
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
      environment: {
        EVENTS_TABLE_NAME: props.eventsTable.tableName,
      },
    });

    // Grant read access to the Kinesis stream
    props.rawStream.grantRead(processorFn);

    // Grant write access to the DynamoDB table
    props.eventsTable.grantWriteData(processorFn);

    // Connect the Kinesis stream as a trigger
    // Use CfnEventSourceMapping directly to avoid tag propagation issues
    // with LocalStack (which expects Tags as a dict, not a list).
    new lambda.CfnEventSourceMapping(this, "KinesisTrigger", {
      functionName: processorFn.functionName,
      eventSourceArn: props.rawStream.streamArn,
      startingPosition: "TRIM_HORIZON",
      batchSize: 100,
    });

    new cdk.CfnOutput(this, "ProcessorLambdaName", {
      value: processorFn.functionName,
      description: "Processing Lambda function name",
    });
  }
}
