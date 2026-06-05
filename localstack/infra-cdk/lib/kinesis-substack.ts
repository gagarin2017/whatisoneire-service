import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as kinesis from "aws-cdk-lib/aws-kinesis";

/**
 * KinesisSubStack provisions the Kinesis stream for raw event data ingestion.
 * This stream collects raw events ingested from various external APIs.
 * The raw data is temporarily buffered here before being processed by EventProcessor.
 */
export class KinesisSubStack extends cdk.NestedStack {
  public readonly rawStream: kinesis.Stream;

  constructor(scope: Construct, id: string, props?: cdk.NestedStackProps) {
    super(scope, id, props);

    // 1. Define the Kinesis Raw Data Stream
    this.rawStream = new kinesis.Stream(this, "EventsRawStream", {
      streamName: "events-raw-stream",
      streamMode: kinesis.StreamMode.ON_DEMAND, // Pay per use, no shard cost
      retentionPeriod: cdk.Duration.hours(24),
    });

    // Output values for easier terminal verification
    new cdk.CfnOutput(this, "StreamArn", {
      value: this.rawStream.streamArn,
      description: "The ARN of the Kinesis raw data stream",
    });
  }
}
