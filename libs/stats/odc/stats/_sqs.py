"""
Work token for SQS based job control
"""
from typing import Optional
from datetime import timedelta, datetime
import toolz
from .model import WorkTokenInterface


class SQSWorkToken(WorkTokenInterface):
    def __init__(self, msg, timeout: int, t0: Optional[datetime] = None):
        if t0 is None:
            t0 = datetime.utcnow()
        self._msg = msg
        self._t0 = t0
        self._deadline = t0 + timedelta(seconds=timeout)

    @property
    def active_seconds(self) -> float:
        """
        :returns: Number of seconds this Token has been active for
        """
        return (datetime.utcnow() - self._t0).total_seconds()

    @property
    def deadline(self) -> datetime:
        """
        Should return timestamp by which work is to be completed
        """
        return self._deadline

    def done(self):
        """
        Called when work is completed successfully
        """
        if self._msg is not None:
            self._msg.delete()
            self._msg = None

    def cancel(self):
        """
        Called when work is terminated for whatever reason without successful result
        """
        if self._msg is None:
            return

        self._msg.change_visibility(VisibilityTimeout=0)
        self._msg = None

    def extend(self, seconds: int) -> bool:
        """
        Called to extend work deadline
        """
        if self._msg is None:
            return False

        new_deadline = datetime.utcnow() + timedelta(seconds=seconds)
        new_timeout = int((new_deadline - self._t0).total_seconds())

        rr = self._msg.change_visibility(VisibilityTimeout=new_timeout)
        ok = toolz.get_in(["ResponseMetadata", "HTTPStatusCode"], rr, default=-1) == 200

        if ok:
            self._deadline = new_deadline

        return ok
