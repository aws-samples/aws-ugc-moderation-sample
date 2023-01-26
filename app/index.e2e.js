const {
    Payload,
    submit,
    getStatus,
    replicateToMIE,
    notifyModerationEvent
} = require(".")

const initialEvent = require("../assets/s3-event.json")

let outputs 

try{
    outputs = require('../assets/cdk-outputs.json')
}catch(error){
    console.log('End to End tests should run only after the stack is deployed.')
    console.log('missing output file in ../assets/cdk-outputs.json')
    console.error(error)
    
    process.exit(-1)
}

const stackName = 'BasupUgcStackStack'

if(!stackName){
    console.error('You should specify your stack name in this process environment')
    process.exit(-1)
}

const Stack = outputs[stackName]

if(!Stack){
    console.error(`Make sure you specified the right stack name. Current stack name = ${stackName}`)
    process.exit(-1)
}

const AWS = require('aws-sdk')
const fsp = require('fs').promises

async function startFlow(){
    
    initialEvent.resources[0] = Stack.InspectionBucketArn
    initialEvent.region = Stack.DeploymentRegion
    initialEvent.detail.bucket.name = Stack.InspectionBucketName
    
    const s3 = new AWS.S3({region: Stack.DeploymentRegion })
    
    const Body = await fsp.readFile('../assets/test.mp4')
    
    const uploadParams = {
        Bucket: Stack.InspectionBucketName,
        Key: 'public/upload/test.mp4',
        Body
    }
    
    console.log('Uploading test file to S3. This will also trigger actual workflow')
    
    try{
        await s3.putObject(uploadParams).promise()
    }catch(error){
        console.log('Could not upload file to S3. Are you sure you have permissions?')
        console.error(error)
        process.exit(-1)
    }
    
    const event_0 = Payload.fromEvent(initialEvent).toObject()
    const event_1 = await submit(event_0)
    
    return event_1
}

async function cleanup(){
    
    const s3 = new AWS.S3({region: Stack.DeploymentRegion })
    const deleteParams = {
        Bucket: Stack.InspectionBucketName,
        Key: 'public/upload/test.mp4'
    }
    
    console.log('Deleting Test S3 file')
    
    try{
        await s3.deleteObject(deleteParams).promise()
    }catch(error){
        console.log('Could not delete file to S3. Are you sure you have permissions?')
        console.error(error)
        process.exit(-1)
    }
    
    return null
    
}

async function delay(payload){
    const { waitSeconds } = payload
    console.log(`waiting for ${waitSeconds} second`)
    await new Promise(
        (resolve, reject) => {
            setTimeout(resolve, waitSeconds * 1000)
        }
    )
    
    return payload
}

const inspect = x => console.log(x) || x
const comment = message => x => console.log(message) || x
const assertEqual = (measuredLabel, expected)  => data =>  {
       const  {
          [measuredLabel]: measured
       } = data
       
       if(measured !== expected){
           throw Error(`For ${measuredLabel} expected: ${expected} but got ${measured}`)
       }
       
       console.log(`Passed Test for ${measuredLabel}`)
       
       return data
        
    }
const assertExists = (measuredLabel)  => data =>  {
       const  {
          [measuredLabel]: measured
       } = data
       
       if(measured === null || measured === undefined){
           throw Error(`For ${measuredLabel} but got ${measured}, but expected non-null`)
       }
       
       console.log(`Passed Test for ${measuredLabel}`)
       return data
        
    }
const assertUnchanged = (measuredLabel) => data => {
       const  {
          [measuredLabel]: measured,
          [`__${measuredLabel}`]: previous
       } = data
       
       if(measured !== previous){
           throw Error(`Assuming ${measuredLabel} is unchanged but received ${measured}; it should have been ${previous} `)
       }
       
       console.log(`Passed Unchanged Test for ${measuredLabel}`)
       return data
}
const cache = key => data => {
    data[`__${key}`] = data[key]
    console.log(`cached ${data[key]} as __${key}`)
    return  data
}

if (!module.parent){
    
    process.env = {
        ... process.env,
        MIE_S3_BUCKET: Stack.DestinationBucketName,
        AWS_REGION: Stack.DeploymentRegion,
        SNS_NOTIFICATION_TOPIC: Stack.SNSNotificationTopicArn,
        WAIT_SECONDS: 3
    }
    
    console.log("***** starting simulated flow *****")
    
    startFlow()
        .then(inspect)
        .then(assertExists("JobId"))
        .then(assertEqual("status", Payload.IN_PROGRESS))
        .then(cache("JobId"))
        .then(delay)
        .then(comment("***** now invoking getStatus *****"))
        .then(getStatus)
        .then(inspect)
        .then(assertUnchanged("JobId"))
        .then(assertEqual("uuid", initialEvent.uuid))
        .then(delay)
        .then(comment("***** invoking getStatus again *****"))
        .then(getStatus)
        .then(inspect)
        .then(assertEqual("uuid", initialEvent.uuid))
        .then(assertUnchanged("JobId"))
        .then(assertEqual("status", Payload.NOT_CLEAR))
        .then(comment("***** replicating to target bucket *****"))
        .then(replicateToMIE)
        .then(inspect)
        .then(assertUnchanged("JobId"))
        .then(assertEqual("uuid", initialEvent.uuid))
        .then(assertEqual("status", Payload.SUCCEEDED))
        .then(comment("***** sending notification to SNS *****"))
        .then(notifyModerationEvent)
        .then(inspect)
        .then(assertUnchanged("JobId"))
        .then(assertEqual("uuid", initialEvent.uuid))
        .then(assertEqual("status", Payload.NOTIFIED))
        .then(cleanup)
        .catch(console.error)

}