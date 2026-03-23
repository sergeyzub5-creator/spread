from __future__ import annotations

from decimal import Decimal


def format_compact_price(value: object) -> str:
    try:
        decimal_value = Decimal(str(value))
    except Exception:
        return "?"

    if decimal_value.is_nan():
        return "?"

    negative = decimal_value < 0
    abs_value = -decimal_value if negative else decimal_value
    text = format(abs_value, "f")
    if "." in text:
        integer_part, fractional_part = text.split(".", 1)
    else:
        integer_part, fractional_part = text, ""

    integer_digits = integer_part.lstrip("0") or "0"

    if abs_value == 0:
        formatted = "0"
    elif integer_digits != "0":
        fraction = fractional_part[:2].rstrip("0")
        formatted = f"{integer_digits}.{fraction}" if fraction else integer_digits
    else:
        leading_zeros = 0
        for ch in fractional_part:
            if ch == "0":
                leading_zeros += 1
                continue
            break

        significant = fractional_part[leading_zeros : leading_zeros + 4].rstrip("0")
        if not significant:
            formatted = "0"
        elif leading_zeros > 2:
            formatted = f"0.{{0}}{significant}"
        else:
            shown_fraction = fractional_part[:4].rstrip("0")
            formatted = f"0.{shown_fraction}" if shown_fraction else "0"

    if negative and formatted != "0":
        return f"-{formatted}"
    return formatted
