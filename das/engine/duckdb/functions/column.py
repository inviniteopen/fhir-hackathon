from duckdb import Expression
from duckdb.typing import VARCHAR

from .common import Case, false, true


def map_to_boolean(col: Expression, to_true: list[str] | str, to_false: list[str] | str) -> Expression:
    """Map the boolean-like string-values to actual boolean."""
    to_true = [to_true] if isinstance(to_true, str) else to_true
    to_false = [to_false] if isinstance(to_false, str) else to_false
    mapped = Case(col.cast(VARCHAR).isin(to_true), true).when(col.cast(VARCHAR).isin(to_false), false)
    return mapped.alias(col.get_name())
