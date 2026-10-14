"""Converts REST rowset values to Python, by the column's Kyuubi type.

The REST API sends ints, doubles and booleans as JSON numbers/booleans, and everything else as
strings. DECIMAL, DATE and TIMESTAMP columns become Decimal, date and datetime. Other types stay
as the engine's text: the DuckDB dialect reports anything outside the basic types as STRING
(lists come back as "[1, 2]"), while Spark sends ARRAY/MAP/STRUCT as JSON text. Spark BINARY text
is lossy (bytes decoded as UTF-8).
"""

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

# Thrift type names from resultsetmetadata, minus "_TYPE"
NUMBER = {"TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL"}
DATETIME = {"DATE", "TIMESTAMP", "TIMESTAMPLOCALTZ"}

_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?$"
)


def type_name(kyuubi_type: str) -> str:
    return kyuubi_type.removesuffix("_TYPE")


def parse_timestamp(text: str) -> datetime:
    # datetime.fromisoformat in 3.10 only takes 3 or 6 fraction digits; engines send 1 to 9
    m = _TIMESTAMP.match(text)
    if not m:
        raise ValueError(f"not a timestamp: {text!r}")
    day, hh, mm, ss, frac, tz = m.groups()
    value = datetime.fromisoformat(day).replace(
        hour=int(hh), minute=int(mm), second=int(ss), microsecond=int((frac or "0")[:6].ljust(6, "0"))
    )
    if tz:
        if tz == "Z":
            return value.replace(tzinfo=timezone.utc)
        sign = 1 if tz[0] == "+" else -1
        digits = tz[1:].replace(":", "")
        offset = timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
        value = value.replace(tzinfo=timezone(sign * offset))
    return value


def convert(kyuubi_type: str, value):
    if value is None:
        return None
    name = type_name(kyuubi_type)
    if not isinstance(value, str):
        return value
    try:
        if name == "DECIMAL":
            return Decimal(value)
        if name == "DATE":
            return date.fromisoformat(value)
        if name in ("TIMESTAMP", "TIMESTAMPLOCALTZ"):
            return parse_timestamp(value)
    except ValueError:
        pass  # e.g. infinity timestamps: keep the text
    return value
