"""Makes TypedLazyFrame look like a proper polars LazyFrame."""

from typing import ClassVar, Self, TypeVar

from pandera.api.polars.model import DataFrameModel
from pandera.errors import SchemaError

import polars as pl

T = TypeVar("T")

class Col[T]:
    name: str
    python_type: type

    def __init__(self, python_type: type[T]): ...
    def __set_name__(self, owner, name: str): ...
    def __get__(self, obj, objtype=None) -> pl.Expr: ...

class TypedLazyFrame(pl.LazyFrame):
    DataFrameModel: ClassVar[type[DataFrameModel]]
    SchemaError: ClassVar[type[SchemaError]]
    _schema_class: ClassVar[type[DataFrameModel]]

    def __init__(self, df: pl.LazyFrame) -> None: ...
    @classmethod
    def from_df(cls, df: pl.LazyFrame, validate: bool = True) -> Self: ...
    @classmethod
    def from_dicts(cls, dicts: list[dict], schema: dict[str, type] | None) -> Self: ...
