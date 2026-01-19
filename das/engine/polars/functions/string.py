import polars as pl


def string_to_boolean(col: pl.Expr, true_values: list[str], false_values: list[str]) -> pl.Expr:
    """Converts a string column to boolean based on matching true and false value lists."""
    return pl.when(col.is_in(true_values)).then(True).when(col.is_in(false_values)).then(False)


def convert_strings_to_boolean(
    df: pl.LazyFrame,
    columns: list[str],
    true_values: list[str],
    false_values: list[str],
) -> pl.LazyFrame:
    """Converts specified columns in the DataFrame from string values to boolean."""
    existing_columns = set(df.columns)
    columns_to_convert = set(columns).intersection(existing_columns)

    return df.with_columns(
        {
            col_name: string_to_boolean(pl.col(col_name), true_values, false_values)
            for col_name in columns_to_convert
        }
    )


def get_string_column_names(df: pl.LazyFrame) -> list[str]:
    """Returns a list of column names with the string data type."""
    return [name for name, dtype in df.schema.items() if dtype == pl.String]


def lowercase_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    """Changes the column names to lowercase format."""
    columns_to_rename = [c for c in df.columns if c != c.lower()]
    return df.rename({c: c.lower() for c in columns_to_rename})


def trim_string_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    """Trims whitespace from all string columns in the DataFrame."""
    string_cols = get_string_column_names(df)
    return df.with_columns(*[pl.col(col_name).str.strip_chars() for col_name in string_cols])


def nullify_string_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    """Replaces empty strings in all string columns with null values."""
    string_cols = get_string_column_names(df)
    return df.with_columns(*[pl.col(col_name).replace("", None) for col_name in string_cols])


def normalize_municipality_name(col: pl.Expr) -> pl.Expr:
    """
    Normalizes Finnish municipality names by capitalizing each part.
    Handles uppercase, lowercase, and hyphenated names.

    Example:
        "MÄNTTÄ-VILPPULA" → "Mänttä-Vilppula"
        "turku" → "Turku"
    """
    return (
        col.str.split("-")
        .list.eval(pl.element().str.to_titlecase())  # capitalize each list element
        .list.join("-")
    )
