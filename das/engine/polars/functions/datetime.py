import polars as pl


def datediff(
    start_date_col: pl.Expr,
    end_date_col: pl.Expr,
    days_to_ignore_col: pl.Expr | None = None,
) -> pl.Expr:
    """
    Calculates the number of days between the `start_date_col` and `end_date_col`.
    Ignores the dates in the array `days_to_ignore_col`.

    :param start_date_col: Represents the start date of the time period.
    :param end_date_col: Represents the end date of the time period.
    :param days_to_ignore_col: Array containing the dates that should be ignored.
    :return: Expression representing the number of days between the start and end dates.
    """
    if days_to_ignore_col is None:
        return (end_date_col - start_date_col).dt.total_days()
    else:
        date_array = pl.date_ranges(start_date_col, end_date_col)
        # Coalesce days_to_ignore with empty list
        days_to_ignore_safe = days_to_ignore_col.fill_null([])
        # Filter out ignored dates
        filtered_dates = date_array.list.set_difference(days_to_ignore_safe)
        # Count: len - 1, minimum 0
        date_count = (filtered_dates.list.len() - 1).clip(lower_bound=0)
        return pl.when(end_date_col.is_null()).then(None).otherwise(date_count)
