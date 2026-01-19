from duckdb import CaseExpression as Case
from duckdb import ColumnExpression as Col
from duckdb import ConstantExpression as Lit
from duckdb import DuckDBPyRelation, Expression
from duckdb import FunctionExpression as F
from duckdb import LambdaExpression as Lambda

false = Lit("FALSE")
none = Lit("NULL")
true = Lit("TRUE")


def with_columns(
    rel: DuckDBPyRelation,
    *cols: Expression,
    **named_cols: Expression,
) -> DuckDBPyRelation:
    """Add or replace columns."""
    # column names for positional and keyword arg expressions
    new_col_names = {expr.get_name() for expr in cols} | set(named_cols)
    # new columns that are explicitly aliased
    aliased_named = [expr.alias(name) for name, expr in named_cols.items()]
    # existing columns that won't be overridden
    existing = [Col(c) for c in rel.columns if c not in new_col_names]
    return rel.select(*existing, *cols, *aliased_named)
