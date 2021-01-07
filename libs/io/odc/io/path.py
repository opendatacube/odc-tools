from pathlib import Path
from os.path import normpath
import os


def default_base_dir():
    """Return absolute path to current directory. If PWD environment variable is
    set correctly return that, note that PWD might be set to "symlinked"
    path instead of "real" path.

    Only return PWD instead of cwd when:

    1. PWD exists (i.e. launched from interactive shell)
    2. Contains Absolute path (sanity check)
    3. Absolute Path in PWD resolves to the same directory as cwd (process didn't call chdir after starting)
    """
    cwd = Path(".").resolve()

    pwd = os.environ.get("PWD")
    if pwd is None:
        return cwd

    pwd = Path(pwd)
    if not pwd.is_absolute():
        return cwd

    if cwd != pwd.resolve():
        return cwd

    return pwd


def normalise_path(p, base=None):
    """Turn path into absolute path resolving any `../` and `.`

    If path is relative pre-pend `base` path to it, `base` if set should be
    an absolute path. If not set, current working directory will be used.
    """
    assert isinstance(p, (str, Path))
    assert isinstance(base, (str, Path, type(None)))

    def norm(p):
        return Path(normpath(p))

    if isinstance(p, str):
        p = Path(p)

    if isinstance(base, str):
        base = Path(base)

    if p.is_absolute():
        return norm(p)

    if base is None:
        base = default_base_dir()
    elif not base.is_absolute():
        raise ValueError("Expect base to be an absolute path")

    return norm(base / p)
