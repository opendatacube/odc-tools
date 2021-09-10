import click
import sys
import logging
from odc.aws.queue import redrive_queue


@click.command("redrive-to-queue")
@click.argument("queue", required=True)
@click.argument("to-queue", required=False)
@click.option(
    "--dryrun", is_flag=True, default=False, help="Don't actually do real work"
)
@click.option(
    "--limit",
    "-l",
    is_flag=False,
    flag_value="default",
    default=None,
    help="Limit the number of messages to transfer.",
)
def cli(queue, to_queue, dryrun, limit):
    """
    Redrives all the messages from the given sqs queue to the destination
    """

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    _log = logging.getLogger(__name__)

    # In case of sending just the flag without parameters this must be applied
    if limit == "default":
        limit = None

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            raise ValueError(f"Limit {limit} is not valid")

        if limit < 1:
            raise ValueError(f"Limit {limit} is not valid.")

    count = redrive_queue(queue, to_queue, limit, dryrun, max_wait=1)

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
