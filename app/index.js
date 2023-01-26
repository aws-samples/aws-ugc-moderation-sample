const {
    RekognitionClient,
    StartContentModerationCommand,
    GetContentModerationCommand
} = require("@aws-sdk/client-rekognition")

const { 
    S3Client, CopyObjectCommand 
} = require("@aws-sdk/client-s3")

const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns")

const uuid = require("uuid")

class Payload {
    
    static INITIALIZING = "INITIALIING"
    static FAILED = "FAILED"
    static IN_PROGRESS = "IN_PROGRESS"
    static CLEAR = "CLEAR"
    static SUCCEEDED = "SUCCEEDED"
    static NOT_CLEAR = "NOT_CLEAR"
    static NOTIFIED = "NOTIFIED"
    
    static FAILED_PARSE_S3 = "Failed to build Payload from event. Missing Bucket Name or Key"
    static WAIT_CONFIG_WARN = "Defaulting to 1 sec wait time."
    
    constructor(event){
        this.event = event
        this.uuid = event.uuid? event.uuid : uuid.v4()
        this.JobId = event.JobId ? event.JobId : null
        this.waitSeconds = event.waitSeconds? event.waitSeconds : null
        this.status = event.status? event.status : null
        
        this.bucketName = event?.detail?.bucket?.name
        this.bucketKey = event?.detail?.object?.key
        
        if(!this.bucketName || !this.bucketKey){
            this.setFailedStatus(Payload.FAILED_PARSE_S3)
        }
        
    }
    
    setFailedStatus(reason){
        if(reason){
            this.failedReason = reason
        }
        this.status = Payload.FAILED
        return this
    }
    setInProgressStatus(){
        this.status = Payload.IN_PROGRESS
        return this
    }
    setClearStatus(){
        this.status = Payload.CLEAR
        return this
    }
    setNotClearStatus(){
        this.status = Payload.NOT_CLEAR
        return this
    }
    setInitializingStatus(){
        this.status = Payload.INITIALIZING
        return this
    }
    setSucceededStatus(){
        this.status = Payload.SUCCEEDED
        return this
    }
    setNotifiedStatus(){
        this.status = Payload.NOTIFIED
        return this
    }
    
    setJobId(jobId){
        this.JobId = jobId
        return this
    }
    
    wait(value){
        let seconds = parseInt(value, 10)
        if(seconds <= 0 ){
            console.warn(Payload.WAIT_CONFIG_WARN)
            seconds = 1
        }
        this.waitSeconds = seconds
        return this
    }
    
    toObject(){
        return {
            ...this.event,
            JobId : this.JobId,
            waitSeconds: this.waitSeconds,
            status: this.status,
            bucket: this.bucketName,
            key: this.bucketKey,
            failureReason: this.failureReason,
            uuid: this.uuid
        }
    }
    
    toString(){
        return JSON.stringify(this.toObject(), null, 2)
    }
    
    static fromEvent(event){
        return new Payload(event)
    }
}

const submit = async event => {
    
    let {
        JOB_TAG,
        WAIT_SECONDS,
        MIN_CONFIDENCE
    } = process.env
    
    if(!JOB_TAG)
        JOB_TAG = "UGCMod-SubmitModerationJob"
        
    if(!WAIT_SECONDS)
        WAIT_SECONDS = 20
        
    if(!MIN_CONFIDENCE)
        MIN_CONFIDENCE = 60
    
    const payload = Payload.fromEvent(event)
    
    if(payload.status === Payload.FAILED){
        console.log(JSON.stringify(event, null, 2))
        console.error(payload.failedReason)
        
        return payload.wait(1).toObject()
    }
    
    const client = new RekognitionClient()
    
    const input = {
        ClientRequestToken: payload.uuid,
        JobTag: JOB_TAG,
        MinConfidence: MIN_CONFIDENCE,
        Video:{
            S3Object: {
                Bucket: payload.toObject().bucket,
                Name: payload.toObject().key
            }
        }
    }
        
    const command = new StartContentModerationCommand(input)
    
    let response
    
    try{
        response = await client.send(command)
    }catch(error){
        payload.setFailedStatus(`Runtime Error: ${error}`)
        console.log(JSON.stringify(event, null, 2))
        console.log(response)
        console.log(input)
        console.error(payload.failedReason)
        
        return payload.wait(1).toObject()
    }
    
    if(!response.JobId){
        payload.setFailedStatus("Runtime Error: failed to get a JobId from Rekognition")
        console.log(JSON.stringify(event, null, 2))
        console.log(response)
        console.log(input)
        console.error(payload.failedReason)
        
        return payload.wait(1).toObject()
    }
    
    payload.setInProgressStatus()
    payload.setJobId(response.JobId)
    
    return payload.wait(WAIT_SECONDS).toObject()
    
}

const getStatus = async event => {
    
    const payload = Payload.fromEvent(event)
    
    if(payload.status === Payload.FAILED){
        console.log(JSON.stringify(event, null, 2))
        console.error(payload.failedReason)
        
        return payload.toObject()
    }
    
    const client = new RekognitionClient()
    
    const input = {
        JobId: payload.toObject().JobId
    }
    
    const command = new GetContentModerationCommand(input)
    
    let response
    
    try{
        response = await client.send(command)
    }catch(error){
        payload.setFailedStatus(`Runtime Error: ${error}`)
        console.log(JSON.stringify(event, null, 2))
        console.log(response)
        console.log(input)
        console.error(payload.failedReason)
        
        return payload.toObject()
    }
    
    const {JobStatus, ModerationLabels} = response
    
    if(ModerationLabels?.length > 0){
        payload.setNotClearStatus()
        return payload.toObject()
    }
    
    if(!JobStatus || JobStatus === Payload.IN_PROGRESS){
        return payload.toObject()
    }
    
    if(JobStatus === Payload.SUCCEEDED && ModerationLabels?.length === 0){
        payload.setClearStatus()
        return payload.toObject()
    }
    
}

const replicateToMIE = async (event, context) => {

    const { MIE_S3_BUCKET, AWS_REGION } = process.env

    const { invokedFunctionArn } = context
    const accountId = invokedFunctionArn.split(':')[4]
    
    const payload = Payload.fromEvent(event)
    
    if(!MIE_S3_BUCKET){
        payload.setFailedStatus("Runtime Error: MIE Destination bucket is not set")
        console.error(payload.failedReason)
        
        return payload.toObject()
    }
    
    if(MIE_S3_BUCKET === payload.toObject().bucket){
        payload.setFailedStatus("Runtime Error: MIE Destination bucket cannot be the same as source bucket")
        console.error(payload.failedReason)
        
        return payload.toObject()
    }
    
    const s3Client = new S3Client({Region: AWS_REGION})
    
    const input = {
        Bucket: MIE_S3_BUCKET,
        Key: payload.toObject().key,
        CopySource: encodeURI(`${payload.toObject().bucket}/${payload.toObject().key}`),
        ExpectedBucketOwner: accountId
    }
    
    const s3CopyCommand = new CopyObjectCommand(input)
    
    let response
    
    try{
        response = await s3Client.send(s3CopyCommand)
    }catch(error){
         payload.setFailedStatus("Runtime Error: Failed to copy object to MIE")
        console.error(payload.failedReason)
        console.log(error)
        
        return payload.wait(1).toObject()
    }
    
    payload.setSucceededStatus()
    return payload.toObject()
}

const notifyModerationEvent = async event => {
     const { SNS_NOTIFICATION_TOPIC } = process.env
    
    const payload = Payload.fromEvent(event)
    
    if(!SNS_NOTIFICATION_TOPIC){
        payload.setFailedStatus("Runtime Error: SNS Notification tipic is not set")
        console.error(payload.failedReason)
        
        return payload.toObject()
    }
    
    const snsClient = new SNSClient();
    
    const payloadObj = payload.toObject()
    
    const input = {
        TopicArn: SNS_NOTIFICATION_TOPIC,
        Subject: `New Moderation event`,
        Message: `New moderation event for ${payloadObj.bucket}/${payloadObj.key}, asset id: ${payloadObj.uuid}`
    }
    
    const publishCommand = new PublishCommand(input)
    
    let response
    
    try{
        response = await snsClient.send(publishCommand)
    }catch(error){
         payload.setFailedStatus("Runtime Error: Failed send SNS notification")
        console.error(payload.failedReason)
        console.log(error)
        
        return payload.wait(1).toObject()
    }
    
    payload.setNotifiedStatus()
    return payload.toObject()
    
    
}

module.exports = {
    Payload,
    submit,
    getStatus,
    replicateToMIE,
    notifyModerationEvent
}
