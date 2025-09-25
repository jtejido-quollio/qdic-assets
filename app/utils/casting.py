import re
from typing import Optional, Any

_NUMERIC_RE = re.compile(r"""^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$""")


def to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = val.strip()
        if not s or s.lower() in {"na", "nan", "null", "none", "inf", "-inf"}:
            return None
        s = s.replace(",", "")  # allow "1,234.56"
        if not _NUMERIC_RE.match(s):
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def to_int(val: Any) -> Optional[int]:
    f = to_float(val)
    if f is None:
        return None
    # only accept if it's effectively an integer
    if abs(f - round(f)) < 1e-9:
        return int(round(f))
    return None
