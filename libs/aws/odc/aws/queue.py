import boto3
from typing import Mapping, Any, Iterable

def get_queue(queue_name: str):
    """
    Return a queue resource by name, e.g., alex-really-secret-queue
    """
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue


def publish_message(queue, message: str, message_attributes: Mapping[str, Any] = {}):
    """
    Publish a message to a queue resource. Message should be a JSON object dumped as a
    string.
    """
    queue.send_message(
        QueueUrl=queue.url,
        MessageBody=message,
        MessageAttributes=message_attributes
    )


def get_messages(queue, limit: bool = None, visibility_timeout: int = 60, message_attributes: Iterable[str] = ["All"]):
    """
    Get messages from a queue resource.
    """
    count = 0
    while True:
        messages = queue.receive_messages(
            VisibilityTimeout=visibility_timeout,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=message_attributes,
        )
        if len(messages) == 0 or (limit and count >= limit):
            break
        else:
            for message in messages:
                count += 1
                if ',' in message.body:
                    msg = message.body.split(',')
                    message = (msg[0], int(msg[1]), int(msg[2]))
                yield message
