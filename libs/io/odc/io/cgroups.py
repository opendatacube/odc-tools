"""
Query Linux cgroup fs for various info
"""
from typing import Optional
from .text import read_int


def get_cpu_quota() -> Optional[float]:
    """
    :returns: ``None`` if unconstrained or there is an error
    :returns: maximum amount of CPU this pod is allowed to use
    """
    quota = read_int("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    if quota is None:
        return None
    period = read_int("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if period is None:
        return None
    return quota / period


def get_mem_quota() -> Optional[int]:
    """
    :returns: ``None`` if there was some error
    :returns: maximum RAM, in bytes, this pod can use according to Linux cgroups

    Note that number returned can be larger than total available memory.
    """
    return read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes")
