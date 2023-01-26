#!/usr/bin/env node
import 'source-map-support/register'
import * as cdk from 'aws-cdk-lib'
import { BasupUgcStackStack } from '../lib/basup-ugc-stack-stack'

const app = new cdk.App()

new BasupUgcStackStack(app, 'BasupUgcStackStack', {})
