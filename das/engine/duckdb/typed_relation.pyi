"""Makes TypedDataFrame look like a proper polars LazyFrame."""

from typing import ClassVar, Self, TypeVar

from duckdb import DuckDBPyRelation, Expression
from pandera.api.pandas.model import DataFrameModel
from pandera.errors import SchemaError

T = TypeVar("T")

class Col[T]:
    name: str
    python_type: type

    def __init__(self, python_type: type[T]): ...
    def __set_name__(self, owner, name: str): ...
    def __get__(self, obj, objtype=None) -> Expression: ...

    DataFrameModel: ClassVar[type[DataFrameModel]]
    SchemaError: ClassVar[type[SchemaError]]
    _schema_class: ClassVar[type[DataFrameModel]]

# faking this as being inherited from DuckDBPyRelation doesn't help
class TypedRelation(DuckDBPyRelation):
    def __init__(self, rel: DuckDBPyRelation) -> None: ...
    @classmethod
    def from_relation(cls, rel: DuckDBPyRelation, validate: bool = True) -> Self: ...
    @classmethod
    def from_dicts(cls, dicts: list[dict]) -> Self: ...
    def with_columns(self, **named_exprs: Expression) -> DuckDBPyRelation: ...
