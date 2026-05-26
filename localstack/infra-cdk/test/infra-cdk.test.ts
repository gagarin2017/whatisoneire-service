import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { DynamoDbSubStack } from "../lib/dynamodb-substack";
import { KinesisSubStack } from "../lib/kinesis-substack";

describe("Infrastructure resource sub-stacks", () => {
  test("creates the Kinesis stream with the expected settings", () => {
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "KinesisTestParent");
    const nested = new KinesisSubStack(stack, "KinesisUnderTest");
    const template = Template.fromStack(nested);

    template.hasResourceProperties("AWS::Kinesis::Stream", {
      Name: "events-raw-stream",
      RetentionPeriodHours: 24,
      StreamModeDetails: {
        StreamMode: "ON_DEMAND",
      },
    });
  });

  test("creates the DynamoDB table with the expected schema", () => {
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "DynamoTestParent");
    const nested = new DynamoDbSubStack(stack, "DynamoUnderTest");
    const template = Template.fromStack(nested);

    template.hasResourceProperties("AWS::DynamoDB::Table", {
      TableName: "irish-events",
      BillingMode: "PAY_PER_REQUEST",
      KeySchema: [
        { AttributeName: "county_id", KeyType: "HASH" },
        { AttributeName: "event_date_id", KeyType: "RANGE" },
      ],
      AttributeDefinitions: [
        { AttributeName: "county_id", AttributeType: "S" },
        { AttributeName: "event_date_id", AttributeType: "S" },
      ],
    });
  });
});
