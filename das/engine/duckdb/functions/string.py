from duckdb import DuckDBPyRelation, Expression

from .common import Col, F, Lambda, Lit, with_columns


def lowercase_columns(rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """Changes the column names to lowercase format."""
    columns_to_change = [c for c in rel.columns if c != c.lower()]
    if not columns_to_change:  # Don't make a new select in case of no transform needed
        return rel
    return rel.select(*[Col(c).alias(c.lower()) for c in rel.columns])


def get_string_column_names(rel: DuckDBPyRelation) -> list[str]:
    """Get names of VARCHAR/string columns."""
    return [name for name, dtype in zip(rel.columns, rel.types, strict=True) if str(dtype) == "VARCHAR"]


def trim_string_columns(rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """Trims whitespace from all string columns."""
    string_cols = get_string_column_names(rel)
    return with_columns(rel, *[F("trim", Col(c)).alias(c) for c in string_cols])


def nullify_string_columns(rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """Replaces empty strings in all string columns with null values."""
    string_cols = get_string_column_names(rel)
    return with_columns(rel, *[F("nullif", Col(c), Lit("")).alias(c) for c in string_cols])


def normalize_municipality_name(col: Expression) -> Expression:
    """
    Normalizes Finnish municipality names by capitalizing each part.
    Handles uppercase, lowercase, and hyphenated names.
    Example:
        "MÄNTTÄ-VILPPULA" → "Mänttä-Vilppula"
        "turku" → "Turku"
    """
    return F(
        "array_to_string",
        F(
            "list_transform",
            F("str_split", col, Lit("-")),
            Lambda("x", F("initcap", Col("x"))),
        ),
        Lit("-"),
    ).alias(col.get_name())
