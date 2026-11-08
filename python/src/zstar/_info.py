"""Parse caller timestamps into archive nanoseconds."""

from decimal import Decimal, InvalidOperation


def _nanoseconds(value: int | float | str) -> int:
    try:
        seconds = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("mtime must be a number") from exc
    if not seconds.is_finite():
        raise ValueError("mtime must be finite")
    parts = seconds.as_tuple()
    assert isinstance(parts.exponent, int)
    result = Decimal((parts.sign, parts.digits, parts.exponent + 9))
    if result != result.to_integral_value():
        raise ValueError("mtime must have nanosecond precision")
    # Check the Decimal before conversion so extreme exponents cannot allocate
    # an unbounded Python integer for a timestamp that will be rejected.
    if not -(1 << 63) <= result < 1 << 63:
        raise OverflowError("mtime is outside the archive timestamp range")
    return int(result)
