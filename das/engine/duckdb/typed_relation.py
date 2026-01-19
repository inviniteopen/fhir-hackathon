from typing import Self, TypeVar

import pandas as pd
from pandera.errors import SchemaError
from pandera.pandas import DataFrameModel

import duckdb

from ..typed_dataframe import ColBase, TypedDataFrameBase

T = TypeVar("T")


class Col[T](ColBase):
    """Polars LazyFrame column descriptor."""

    def __get__(self, obj, objtype=None) -> duckdb.Expression:
        """
        Return DuckDB column reference when accessed via instance OR class.
        Argument `obj` is ignored in both cases.
        """
        return duckdb.ColumnExpression(self.name)


class TypedRelation(TypedDataFrameBase, abstract=True):
    """
    Base class for typed DuckDB relation.
    Wraps duckdb.DuckDBPyRelation for full DuckDB functionality.
    """

    DataFrameModel = DataFrameModel
    SchemaError = SchemaError

    # public API

    @classmethod
    def from_relation(cls, rel: duckdb.DuckDBPyRelation, validate: bool = True) -> Self:
        """Create typed dataframe instance from dataframe if its schema matches Col definitions."""
        return cls(rel)

    @classmethod
    def from_dicts(cls, dicts: list[dict]) -> Self:
        return cls.from_relation(duckdb.from_df(pd.DataFrame(dicts)))

    def with_columns(self, **named_exprs: duckdb.Expression) -> duckdb.DuckDBPyRelation:
        """
        Add or replace columns, keeping all existing columns.

        Usage:
            rel = rel.with_columns(
                full_name=FunctionExpression("concat", ColumnExpression("first"), ColumnExpression("last")),
                amount_clean=FunctionExpression("trim", ColumnExpression("amount")),
            )
        """
        existing = [duckdb.ColumnExpression(c) for c in self.columns if c not in named_exprs]
        new = [expr.alias(name) for name, expr in named_exprs.items()]
        return self.select(*existing, *new)
