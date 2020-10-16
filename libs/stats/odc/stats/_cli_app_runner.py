import boto3

def get_queue(queue_name):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue

def publish_message(queue):
    session = boto3.Session()
    sqs = session.client('sqs')
    resp = sqs.send_message(
        QueueUrl=queue.url,
        MessageBody=(
            'Sample message for Queue.'
        )
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

def process_queue(queue):
    for msg in get_messages(queue, 2):
        print(msg)

@main.command('run_command')
@click.argument('command', type=str, nargs=1)
@click.argument('db', type=str, nargs=1)
@click.argument('queue', type=str, nargs=-1)

def run_command(command, db, queue):
    queue = get_queue(queue)
    # "deafrica-prod-eks-stats-geomedian"
    messages = get_messages(queue, limit)
    for message in messages:
        print(message)