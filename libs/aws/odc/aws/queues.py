import boto3

def get_queue(queue_name):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue

def publish_message(queue, msg):
    resp = queue.send_message(
        QueueUrl=queue.url,
        MessageBody=msg
    )
    assert(resp['ResponseMetadata']['HTTPStatusCode'] == 200)

def get_messages(queue, limit):
    count = 0
    while True:
        messages = queue.receive_messages(
            VisibilityTimeout=60,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )

        if len(messages) == 0 or (limit and count >= limit):
            break
        else:
            for message in messages:
                count += 1
                yield message