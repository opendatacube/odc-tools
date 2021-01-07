import boto3
import itertools
from typing import Mapping, Any, Iterable, Optional


def get_queue(queue_name: str):
    """
    Return a queue resource by name, e.g., alex-really-secret-queue
    """
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue


def publish_message(queue, message: str, message_attributes: Mapping[str, Any] = {}):
    """
    Publish a message to a queue resource. Message should be a JSON object dumped as a
    string.
    """
    queue.send_message(
        QueueUrl=queue.url, MessageBody=message, MessageAttributes=message_attributes
    )


def publish_messages(queue, messages):
    """
    Publish messages to a queue resource.
    """
    queue.send_messages(Entries=messages)


def _sqs_message_stream(queue, **kw):
    while True:
        messages = queue.receive_messages(**kw)
        if len(messages) == 0:
            return

        for msg in messages:
            yield msg


def get_messages(
    queue,
    limit: Optional[int] = None,
    visibility_timeout: int = 60,
    message_attributes: Iterable[str] = ["All"],
    max_wait: int = 10,
    **kw
):
    """
    Get messages from SQS queue resource. Returns a lazy sequence of message objects.

    :queue: queue URL
    :param limit: the maximum number of messages to return from the queue (default to all)
    :param visibility_timeout: A period of time in seconds during which Amazon SQS prevents other consumers
                               from receiving and processing the message
    :message_attributes: Select what attributes to include in the messages, default All
    :max_wait: Longest to wait in seconds before assuming queue is empty (default: 10)
    :**kw: Any other arguments are passed to ``.receive_messages()`` boto3 call

    :return: Iterator of sqs messages
    """
    messages = _sqs_message_stream(
        queue,
        VisibilityTimeout=visibility_timeout,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=max_wait,
        MessageAttributeNames=message_attributes,
        **kw
    )
    if limit is None or limit == 0:
        return messages

    return itertools.islice(messages, limit)
