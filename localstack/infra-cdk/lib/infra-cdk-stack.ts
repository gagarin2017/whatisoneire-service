import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { KinesisSubStack } from './kinesis-substack';
import { LambdasSubStack } from './lambdas-substack';
import { DynamoDbSubStack } from './dynamodb-substack';

/**
 * Root Infrastructure Stack for WhatsOnEire services.
 * Orchestrates resource groups through nested sub-stacks:
 * - KinesisSubStack: provisions raw stream buffering.
 * - LambdasSubStack: provisions ingestion schedulers & Lambda handlers.
 * - DynamoDbSubStack: provisions normalized storage table.
 */
export class InfraCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Stack-level description
    this.templateOptions.description = "WhatsOnEire Infrastructure Stack - Kinesis Streams, Ingestion Lambdas, DynamoDB Tables and EventBridge Scheduler Rules.";

    // Apply global Tags to propagate down to all resources in all sub-stacks
    cdk.Tags.of(this).add('Project', 'WhatsOnEire');
    cdk.Tags.of(this).add('Environment', 'local');

    // 1. Instantiate the Kinesis Sub-stack
    const kinesisStack = new KinesisSubStack(this, 'KinesisSubStack');

    // 2. Instantiate the Lambdas Sub-stack (depends on Kinesis stream)
    new LambdasSubStack(this, 'LambdasSubStack', {
      rawStream: kinesisStack.rawStream,
    });

    // 3. Instantiate the DynamoDB Sub-stack
    new DynamoDbSubStack(this, 'DynamoDbSubStack');
  }
}