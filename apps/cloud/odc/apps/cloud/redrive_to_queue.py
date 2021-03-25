import click
import sys
import logging
from odc.aws.queue import redrive_queue


@click.command("redrive-to-queue")
@click.argument("queue", required=True)
@click.argument("to-queue", required=False)
@click.option(
    "--limit",
    "-l",
    type=int,
    help="Limit the number of messages to transfer.",
    default=None,
)
@click.option(
    "--dryrun",
    is_flag=True,
    default=False,
    help="Don't actually do real work"
)
def cli(queue, to_queue, limit, dryrun):
    """
    Redrives all the messages from the given sqs queue to the destination
    """

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    _log = logging.getLogger(__name__)

    count = redrive_queue(queue, to_queue, limit, dryrun)

    if count == 0:
        _log.info("No messages to redrive")
        return

    if not dryrun:
        _log.info(f"Completed sending {count} messages to the queue")
    else:
        _log.warning(
            f"DRYRUN enabled, would have pushed approx {count} messages to the queue"
        )


if __name__ == "__main__":
    cli()
