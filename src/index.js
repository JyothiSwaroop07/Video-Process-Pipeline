import { ReceiveMessageCommand, SQSClient, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import dotenv from 'dotenv';
dotenv.config();

const client = new SQSClient({
    region: 'us-east-1',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
});

const ecsClient = new ECSClient({
    region: 'us-east-1',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
})

async function init() {
    const command = new ReceiveMessageCommand({
        QueueUrl: "https://sqs.us-east-1.amazonaws.com/438600562151/rawVideoBucketQueueTestimonialhub",
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
    });

    while(true) {
        const {Messages} = await client.send(command);
        if(!Messages){
            console.log("No Message in queue");
            continue;
        }

        try {
            for(const message of Messages) {
                const {MessageId, Body} = message;
                console.log(`message received`, {MessageId, Body});

                if(!Body) continue;

                // Validate with JSDoc type hint
                /** @type {import("aws-lambda").S3Event} */
                const event = JSON.parse(Body);

                if("Service" in event && "Event" in event) {
                    if(event.Event === "s3:TestEvent"){
                        await client.send(new DeleteMessageCommand({QueueUrl: 'https://sqs.us-east-1.amazonaws.com/438600562151/rawVideoBucketQueueTestimonialhub',
                            ReceiptHandle: message.ReceiptHandle, 
                        }))
                        continue;
                    } 
                }

                // Process records
                for(const record of event.Records) {
                    const {s3} = record;
                    const {bucket, object: {key}} = s3;

                    const runTaskCommand = new RunTaskCommand({
                        taskDefinition: 'arn:aws:ecs:us-east-1:438600562151:task-definition/video-transcoder',
                        cluster: 'arn:aws:ecs:us-east-1:438600562151:cluster/Dev',
                        launchType: "FARGATE",
                        networkConfiguration: {
                            awsvpcConfiguration: {
                                assignPublicIp: "ENABLED",
                                securityGroups: ['sg-053a83c1d56034f62'],
                                subnets: ['subnet-0c1596b859026bf35', 'subnet-04a7060a7aa08b431', 'subnet-041d8ac65d8cdd273'],
                            }
                        },
                        overrides: {
                            containerOverrides: [{name: 'video-transcoder', environment: [{name: 'BUCKET_NAME', value: bucket.name}, 
                                {name: 'KEY', value: key}
                            ]}]
                        }
                    })

                    await ecsClient.send(runTaskCommand);

                    // Additional processing here
                    await client.send(new DeleteMessageCommand({QueueUrl: 'https://sqs.us-east-1.amazonaws.com/438600562151/rawVideoBucketQueueTestimonialhub',
                        ReceiptHandle: message.ReceiptHandle, 
                    }))

                }

                
            }
        } catch(err) {
            console.log(err);
        }
    }
}

init();
