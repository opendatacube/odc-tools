from datetime import datetime, timedelta
from odc.stats._sqs import SQSWorkToken
from odc.aws.queue import get_queue, publish_message
from odc.stats.tasks import TaskReader, render_task
from odc.stats.model import OutputProduct


def test_sqs_work_token(sqs_message):
    tk = SQSWorkToken(sqs_message, 60)

    assert tk.active_seconds < 2
    assert tk.start_time < datetime.utcnow()
    assert tk.deadline > datetime.utcnow()

    deadline0 = tk.deadline
    assert tk.extend_if_needed(1000, 1)
    assert tk.deadline == deadline0
    assert tk.extend_if_needed(100, 60)
    assert tk.deadline > deadline0

    deadline0 = tk.deadline
    assert tk.extend(200)
    assert tk.deadline > deadline0

    tk.done()
    assert tk._msg is None
    # should be no-op
    tk.done()
    tk.cancel()
    assert tk.extend(100) is False

    tk = SQSWorkToken(sqs_message, 60)

    assert tk.active_seconds < 2
    assert tk.deadline > datetime.utcnow()
    tk.cancel()
    assert tk._msg is None
    # should be no-op
    tk.done()
    tk.cancel()
    assert tk.extend(100) is False


def test_rdr_sqs(sqs_queue_by_name, test_db_path):
    q = get_queue(sqs_queue_by_name)
    product = OutputProduct.dummy()
    rdr = TaskReader(test_db_path, product)

    for tidx in rdr.all_tiles:
        publish_message(q, render_task(tidx))

    for task in rdr.stream_from_sqs(sqs_queue_by_name, visibility_timeout=120, max_wait=0):
        _now = datetime.utcnow()
        assert task.source is not None
        assert task.source.active_seconds < 2
        assert task.source.deadline > _now
        assert task.source.deadline < _now + timedelta(seconds=120+10)

        task.source.extend(3600)
        assert task.source.deadline > _now
        assert task.source.deadline < _now + timedelta(seconds=3600+10)
        task.source.done()
