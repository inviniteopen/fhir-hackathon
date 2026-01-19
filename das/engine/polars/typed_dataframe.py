from typing import Self, TypeVar

from pandera.api.polars.model import DataFrameModel
from pandera.errors import SchemaError

import polars as pl

from ..typed_dataframe import ColBase, TypedDataFrameBase

T = TypeVar("T")


class Col[T](ColBase):
    """Polars LazyFrame column descriptor."""

    def __get__(self, obj, objtype=None) -> pl.Expr:
        """
        Return Polars column reference when accessed via instance OR class.
        Argument `obj` is ignored in both cases.
        """
        return pl.col(self.name)


class TypedLazyFrame(TypedDataFrameBase, abstract=True):
    """
    Base class for typed polars LazyFrame.
    Wraps pl.LazyFrame for full Polars functionality.
    """

    DataFrameModel = DataFrameModel
    SchemaError = SchemaError

    @classmethod
    def from_df(cls, df: pl.LazyFrame, validate: bool = True) -> Self:
        """Create typed dataframe instance from dataframe if its schema matches Col definitions."""
        if validate:
            cls._schema_class.validate(df)
        return cls(df)

    @classmethod
    def from_dicts(cls, dicts: list[dict], schema) -> Self:
        return cls.from_df(pl.from_dicts(dicts, schema).lazy())
