from typing import ClassVar, Self, TypeVar

from pyspark.errors.exceptions.base import PySparkAssertionError
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.column import Column as SparkColumn
from pyspark.testing import assertSchemaEqual
from sparkdantic import SparkModel

from das.engine.typed_dataframe import ColBase, TypedDataFrameBase

T = TypeVar("T")


class Col[T](ColBase):
    """Spark DataFrame column descriptor."""

    def __get__(self, obj, objtype=None) -> SparkColumn:
        """Return Spark column reference when accessed via instance"""
        if obj is None:
            raise AttributeError(f"Cannot access column {self.name} via class, use F.col() instead.")
        return obj.__wrapped__[self.name]


class TypedDataFrame(TypedDataFrameBase, abstract=True):
    """
    Base class for typed pyspark DataFrame.
    Wraps pyspark.sql.DataFrame for full Spark functionality.
    """

    DataFrameModel = SparkModel
    SchemaError = PySparkAssertionError
    _schema_class: ClassVar[type[SparkModel]]

    @classmethod
    def from_df(cls, df: SparkDataFrame, validate: bool = True, ignore_nullable: bool = True) -> Self:
        """Create TypedDataFrame instance from dataframe if its schema matches Column definitions."""
        if validate:
            assertSchemaEqual(
                df.schema,
                cls.as_spark_schema(),
                ignoreNullable=ignore_nullable,
                ignoreColumnOrder=True,
            )
        return cls(df)

    @classmethod
    def as_spark_schema(cls):
        """Generate PySpark StructType schema from the TypedDataFrame definition."""
        return cls._schema_class.model_spark_schema(safe_casting=True)

    @classmethod
    def from_dicts(cls, dicts: list[dict]) -> Self:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession found. Create a SparkSession first.")
        df = spark.createDataFrame(dicts, schema=cls.as_spark_schema())  # type: ignore
        return cls.from_df(df)
