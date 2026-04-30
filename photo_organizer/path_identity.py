"""
Path identity helpers for durable catalog state.
"""
from pathlib import Path
from typing import Optional, Union

PathLike = Union[str, Path]


def normalize_path_key(path: Optional[PathLike]) -> Optional[str]:
    """Return a stable lookup key for a filesystem path.

    This intentionally uses lexical normalization rather than resolving
    symlinks/junctions. It preserves operator intent for removable drives and
    avoids filesystem access during catalog migrations.
    """
    if path is None:
        return None
    value = str(path).replace("/", "\\").rstrip("\\/")
    return value.casefold()
