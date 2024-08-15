import * as cdk from 'aws-cdk-lib'
import { Construct } from 'constructs'
import * as S3 from 'aws-cdk-lib/aws-s3'
import * as Events from 'aws-cdk-lib/aws-events'
import * as Lambda from 'aws-cdk-lib/aws-lambda'
import * as Targets from 'aws-cdk-lib/aws-events-targets'
import * as SFn from 'aws-cdk-lib/aws-stepfunctions'
import * as SFnTasks from 'aws-cdk-lib/aws-stepfunctions-tasks'
import * as SNS from 'aws-cdk-lib/aws-sns'
import * as IAM from 'aws-cdk-lib/aws-iam'
import * as Logs from 'aws-cdk-lib/aws-logs'
import * as path from 'path'

export class BasupUgcStackStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props)
    
    /* STACK PARAMETERS */
    const replicationBucketName = new cdk.CfnParameter(this, 'replicationBucketName', {
      description: "Amazon S3 bucket name where to duplicate content for MIE ingestion",
      type: "String"
    })
    
    const notificationEmailAddress = new cdk.CfnParameter(this, 'notificationEmailAddress', {
      description: "Email address where to send emails in case of a moderation event",
      type: "String"
    })
    
    /* STACK UTILITIES */
    const snsNotificationTopic = new SNS.Topic(this, "UGCModNotificationTopic", {
      displayName:"UGC Moderation Topic",
      fifo: false
    })

    
    const subscription = new SNS.Subscription(this, "UGCModerationSubscription", {
      protocol: SNS.SubscriptionProtocol.EMAIL,
      endpoint: notificationEmailAddress.valueAsString,
      topic: snsNotificationTopic
    })
    
    const UGCInspectionBucket = new S3.Bucket(this, 'UGCInspectionBucket', {
      eventBridgeEnabled: true,
      enforceSSL: true,
      blockPublicAccess: {
        blockPublicAcls: true,
        blockPublicPolicy: true,
        ignorePublicAcls: true,
        restrictPublicBuckets: true
      },
      encryption: S3.BucketEncryption.S3_MANAGED

    })
    
    UGCInspectionBucket.grantRead(new IAM.ServicePrincipal("rekognition.amazonaws.com"))
    
    const MIEReplicationBucket = S3.Bucket.fromBucketName(
      this, "MIEReplicationBucket", replicationBucketName.valueAsString 
    )
    
    const UGCInspectionRule = new Events.Rule(this, 'UGCInspectionRule', {
      eventPattern:{
      "source": ["aws.s3"],
      "detailType": ["Object Created"],
      "detail": {
        "bucket": {
          "name": [UGCInspectionBucket.bucketName]
        },
        "object": {
          "key": [{
            "prefix": "public/upload/"
          }]
        }
      }
}
    })
    
    /* WORKFLOW LAMBDA FUNCTIONS */
    
    const submitLambda = new Lambda.Function(this, 'UGCModSubmitJobLambda', {
      runtime: Lambda.Runtime.NODEJS_20_X,
      code: Lambda.Code.fromAsset(path.join(__dirname, "../app" )),
      architecture: Lambda.Architecture.ARM_64,
      handler: 'index.submit',
      timeout: cdk.Duration.minutes(3)
    })
    
    const submitRekognitionJobPolicyStatement = new IAM.PolicyStatement({
      actions: [
        "rekognition:StartContentModeration"
        ],
        resources: ["*"]
    })
    
    const submitRekognitionJobPolicy = new IAM.Policy(this, 'UGCMod-Reko-Put', {
      statements: [
          submitRekognitionJobPolicyStatement
        ]
    })
    
    submitLambda.role?.attachInlinePolicy(submitRekognitionJobPolicy)
    UGCInspectionBucket.grantRead(submitLambda)
    
    const getStatusLambda = new Lambda.Function(this, 'UGCModGetStatusLambda', {
      runtime: Lambda.Runtime.NODEJS_20_X,
      code: Lambda.Code.fromAsset(path.join(__dirname, "../app" )),
      architecture: Lambda.Architecture.ARM_64,
      handler: 'index.getStatus',
      timeout: cdk.Duration.minutes(3)
    })
    
    const getRekognitionJobPolicyStatement = new IAM.PolicyStatement({
      actions: [
        "rekognition:GetContentModeration"
        ],
        resources: ["*"]
    })
    
    const getRekognitionJobPolicy = new IAM.Policy(this, 'UGCMod-Reko-Read', {
      statements: [
          getRekognitionJobPolicyStatement
        ]
    })
    
    getStatusLambda.role?.attachInlinePolicy(getRekognitionJobPolicy)
    
    
    const replicateToMIELambda = new Lambda.Function(this, 'UGCModReplicateToMIELambda', {
      runtime: Lambda.Runtime.NODEJS_20_X,
      code: Lambda.Code.fromAsset(path.join(__dirname, "../app" )),
      architecture: Lambda.Architecture.ARM_64,
      handler: 'index.replicateToMIE',
      timeout: cdk.Duration.minutes(3),
      environment: {
        MIE_S3_BUCKET: MIEReplicationBucket.bucketName
      }
    })
    
    UGCInspectionBucket.grantRead(replicateToMIELambda)
    MIEReplicationBucket.grantWrite(replicateToMIELambda)
    
    const notifyModerationEventLambda = new Lambda.Function(this, 'UGCnotifyModerationEventLambda', {
      runtime: Lambda.Runtime.NODEJS_20_X,
      code: Lambda.Code.fromAsset(path.join(__dirname, "../app" )),
      architecture: Lambda.Architecture.ARM_64,
      handler: 'index.notifyModerationEvent',
      timeout: cdk.Duration.minutes(3),
      environment:{
        SNS_NOTIFICATION_TOPIC: snsNotificationTopic.topicArn
      }
    })
    
    snsNotificationTopic.grantPublish(notifyModerationEventLambda)
    
    /* STEPFUNCTIONS WORKFLOW */
    
    const submitJob = new SFnTasks.LambdaInvoke(this, 'Submit Job', {
      lambdaFunction: submitLambda,
      outputPath: '$.Payload',
    })

    const waitX = new SFn.Wait(this, 'Wait X Seconds', {
      time: SFn.WaitTime.secondsPath('$.waitSeconds'),
    })

    const getStatus = new SFnTasks.LambdaInvoke(this, 'Get Job Status', {
      lambdaFunction: getStatusLambda,
      outputPath: '$.Payload',
    })
    
    const replicateToMIE = new SFnTasks.LambdaInvoke(this, 'Replicate to MIE Task', {
      lambdaFunction: replicateToMIELambda,
      outputPath: '$.Payload',
    })
    
    const notifyModerationEvent = new SFnTasks.LambdaInvoke(this, 'Notify Moderation Event Task', {
      lambdaFunction: notifyModerationEventLambda,
      outputPath: '$.Payload',
    })

  const jobFailed = new SFn.Fail(this, 'Job Failed', {
    cause: 'Rekognition Job failed',
    error: 'DescribeJob returned FAILED',
  })
  
  const jobSucceeded = new SFn.Succeed(this, 'Job Succeeded', {
    comment: "Two possible success states: check for status. NOTIFIED | SUCCEEDED"
  })

  const definition = submitJob
    .next(waitX)
    .next(getStatus)
    .next(new SFn.Choice(this, 'Job Complete?')
      .when(SFn.Condition.stringEquals('$.status', 'FAILED'), jobFailed)
      .when(SFn.Condition.stringEquals('$.status', 'CLEAR'), replicateToMIE.next(jobSucceeded))
      .when(SFn.Condition.stringEquals('$.status', 'NOT_CLEAR'), notifyModerationEvent.next(jobSucceeded))
      .otherwise(waitX))

  const UGCModStateMachine = new SFn.StateMachine(this, 'StateMachine', {
    definition,
    timeout: cdk.Duration.minutes(30),
    tracingEnabled: true,
    logs:{
      level: SFn.LogLevel.ALL,
      destination: new Logs.LogGroup(this, 'UGCModStateMachineLogs')
    }
  })
  
  // Create an IAM role for Events to start the State Machine
    const eventsRole = new IAM.Role(this, 'UGCModEventsRuleRole', {
      assumedBy: new IAM.ServicePrincipal('events.amazonaws.com')
    })
    
  UGCModStateMachine.grantStartExecution(eventsRole)
  
  const ruleTarget = new Targets.SfnStateMachine(UGCModStateMachine, {
    role: eventsRole
  })
  
  UGCInspectionRule.addTarget(ruleTarget)
  
  new cdk.CfnOutput(this, 'InspectionBucketName', {
    value:UGCInspectionBucket.bucketName
  })
  new cdk.CfnOutput(this, 'DestinationBucketName', {
    value:replicationBucketName.valueAsString
  })
  new cdk.CfnOutput(this, 'InspectionBucketArn', {
    value:UGCInspectionBucket.bucketArn
  })
  new cdk.CfnOutput(this, 'DestinationBucketArn', {
    value: MIEReplicationBucket.bucketArn
  })
  new cdk.CfnOutput(this, 'NotificationEmailAddress', {
    value: notificationEmailAddress.valueAsString
  })
  new cdk.CfnOutput(this, 'SNSNotificationTopicArn', {
    value: snsNotificationTopic.topicArn
  })
  new cdk.CfnOutput(this, 'DeploymentRegion', {
    value: cdk.Stack.of(this).region
  })
    
  }
}
