import boto3
import itertools
from typing import Mapping, Any, Iterable, Optional


def redrive_queue(
    queue_name: str,
    to_queue_name: Optional[str] = None,
    limit: Optional[int] = 0,
    dryrun: bool = False,
):
    """
    Redrive messages from one queue to another. Default usage is to define
    a "deadletter" queue, and pick its "alive" counterpart, and redrive
    messages to that queue.
    """
    dead_queue = get_queue(queue_name)
    alive_queue = None

    if to_queue_name is not None:
        alive_queue = get_queue(to_queue_name)
    else:
        source_queues = list(dead_queue.dead_letter_source_queues.all())
        if len(source_queues) == 0:
            raise Exception(
                "No alive queue found for the deadletter queue, please check your configuration."
            )
        elif len(source_queues) > 1:
            raise Exception(
                "Deadletter queue has more than one source, please specify the target queue name."
            )
        alive_queue = source_queues[0]

    messages = get_messages(dead_queue)

    count_messages = int(dead_queue.attributes.get("ApproximateNumberOfMessages"))

    # If there's no messages then there's no work to do. If it's a dryrun, we
    # don't do anything either.
    if count_messages == 0 or dryrun:
        return count_messages

    count = 0
    for message in messages:
        # Assume this works. Exception handling elsewhere.
        alive_queue.send_message(MessageBody=message.body)
        message.delete()
        count += 1
        if limit and count >= limit:
            break

    # Return the number of messages that were redriven.
    return count


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
