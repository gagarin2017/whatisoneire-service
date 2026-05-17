#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { InfraCdkStack } from '../lib/infra-cdk-stack';

const app = new cdk.App();
new InfraCdkStack(app, 'InfraCdkStack', {
  env: {
    account: '000000000000',
    region: 'eu-west-1',
  },

  // NOTE: was removed as awslocal diff was throwing an error when ran locally
  // synthesizer: new cdk.BootstraplessSynthesizer(),
});
