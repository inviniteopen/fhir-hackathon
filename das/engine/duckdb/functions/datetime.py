from duckdb import DuckDBPyRelation, Expression
from duckdb.typing import DATE, VARCHAR

from .common import Case, Col, F, Lambda, Lit, none, true, with_columns


def convert_int_expr_to_date(col: Expression) -> Expression:
    col_as_str = col.cast(VARCHAR)
    is_valid = F("regexp_matches", col_as_str, Lit(r"^\d{8}$"))
    parsed = F("strptime", col_as_str, Lit("%Y%m%d"))
    return (Case(is_valid, parsed.cast(DATE))).alias(col.get_name())


def convert_ints_to_dates(
    rel: DuckDBPyRelation, columns: list[str]
) -> DuckDBPyRelation:
    return with_columns(rel, *[convert_int_expr_to_date(Col(col)) for col in columns])


def timestamp_to_date(
    col: Expression, timestamp_format: str | None = None
) -> Expression:
    """Converts timestamp expression to date with optional format and timezone normalization."""
    if timestamp_format:
        parsed = F("strptime", col, Lit(timestamp_format))
        return parsed.cast(DATE).alias(col.get_name())
    else:
        return col.cast(DATE).alias(col.get_name())


def convert_timestamps_to_dates(
    rel: DuckDBPyRelation,
    columns: list[str],
    timestamp_format: str | None = None,
) -> DuckDBPyRelation:
    """Converts timestamp columns to date columns with optional format and timezone normalization."""
    columns_to_check = set(columns).intersection(rel.columns)
    if not columns_to_check:
        return rel

    return with_columns(
        rel,
        *[timestamp_to_date(Col(col), timestamp_format) for col in columns_to_check],
    )


def datediff(
    start_date_col: Expression,
    end_date_col: Expression,
    days_to_ignore_col: Expression | None = None,
) -> Expression:
    """
    Calculates the number of days between the `start_date_col` and `end_date_col`.
    Ignores the dates in the array `days_to_ignore_col`.

    :param start_date_col: Represents the start date of the time period.
    :param end_date_col: Represents the end date of the time period.
    :param days_to_ignore_col: Array containing the dates that should be ignored.
    :return: Expression representing the number of days between the start and end dates.
    """
    if days_to_ignore_col is None:
        # date_diff('day', start, end)
        return F("date_diff", Lit("day"), start_date_col, end_date_col)
    else:
        # Generate date range: generate_series(start, end, INTERVAL 1 DAY)
        date_array = F("generate_series", start_date_col, end_date_col, Lit("1 day"))

        # Coalesce days_to_ignore with empty array
        days_to_ignore_safe = F("coalesce", days_to_ignore_col, F("list_value"))

        # array_except: list_filter to exclude ignored days
        # DuckDB: list_filter(date_array, x -> NOT list_contains(ignore_list, x))
        filtered_dates = F(
            "list_filter",
            date_array,
            Lambda(
                "x",
                F(
                    "not",
                    F("list_contains", days_to_ignore_safe, Col("x")),
                ),
            ),
        )

        # len(filtered) - 1, minimum 0
        date_count = F("greatest", F("len", filtered_dates) - Lit("1"), Lit("0"))

        # Handle null end_date
        return Case(F("isnull", end_date_col), none).otherwise(date_count)
