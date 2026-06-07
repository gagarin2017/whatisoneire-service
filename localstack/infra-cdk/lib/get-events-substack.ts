import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as path from "path";

export interface GetEventsSubStackProps extends cdk.NestedStackProps {
  readonly eventsTable: dynamodb.ITable;
}

/**
 * GetEventsSubStack provisions the read-API Lambda that queries DynamoDB.
 *
 * This Lambda is invoked by API Gateway (or HttpLambdaRunner locally) when the
 * frontend requests events. It reads from the `irish-events` table and returns
 * the results as JSON.
 */
export class GetEventsSubStack extends cdk.NestedStack {
  constructor(scope: Construct, id: string, props: GetEventsSubStackProps) {
    super(scope, id, props);

    const jarPath = path.join(
      __dirname,
      "../../../infra/target/scala-3.3.5/whats-on-eire-infra-assembly-0.1.0-SNAPSHOT.jar",
    );

    // Read-API Lambda — queries DynamoDB and returns events as JSON
    const getEventsFn = new lambda.Function(this, "GetEventsHandler", {
      runtime: lambda.Runtime.JAVA_21,
      handler: "what.is.on.eire.GetEventsLambdaHandler",
      code: lambda.Code.fromAsset(jarPath),
      timeout: cdk.Duration.seconds(15),
      memorySize: 512,
      environment: {
        EVENTS_TABLE_NAME: props.eventsTable.tableName,
      },
    });

    // Grant read-only access to the DynamoDB table
    props.eventsTable.grantReadData(getEventsFn);

    new cdk.CfnOutput(this, "GetEventsLambdaName", {
      value: getEventsFn.functionName,
      description: "GetEvents Lambda function name",
    });
  }
}
