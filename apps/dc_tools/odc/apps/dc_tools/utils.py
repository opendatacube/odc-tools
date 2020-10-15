
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