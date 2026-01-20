import polars as pl
import pytest

from das.engine.polars.typed_dataframe import Col, TypedLazyFrame


class Point(TypedLazyFrame):
    x: Col[int | None]
    y: Col[int | None]
    color: Col[str | None]


class Point3D(Point):
    z: Col[int | None]


class FillColor(TypedLazyFrame):
    fillcolor: Col[str | None]


class Point4D(Point3D, FillColor):
    t: Col[int | None]


class IntList(TypedLazyFrame):
    ints: Col[list[int]]


def test_as_polars_lazyframe():
    df = pl.LazyFrame(
        {
            "x": [0, 0, 0],
            "y": [0, -1, 1],
            "color": ["black", "blue", "red"],
        }
    )
    point = Point.from_df(df)
    assert point.select(Point.color).collect().to_dicts() == [
        {"color": "black"},
        {"color": "blue"},
        {"color": "red"},
    ]


def test_column_access_via_class():
    # Test that we can access columns via the class, not just instance
    df = pl.LazyFrame({"x": [1, 2], "y": [3, 4], "color": ["red", "blue"]})
    point = Point.from_df(df)
    # Should be able to use Point.x even without an instance
    result = point.select(Point.x, Point.color).collect()
    assert result.to_dicts() == [{"x": 1, "color": "red"}, {"x": 2, "color": "blue"}]


def test_self_join():
    df = pl.LazyFrame(
        {
            "x": [0, 0, 0],
            "y": [0, -1, 1],
            "color": ["black", "blue", "red"],
        }
    )
    point1 = Point.from_df(df)
    point2 = Point.from_df(df)
    joined = point1.join(point2, on=Point.x, how="inner")
    assert joined.collect().height == 3 * 3


def test_with_int_list():
    df = pl.LazyFrame({"ints": [[0, -1, 1], [1, 2, 3]]})
    int_list = IntList.from_df(df)
    assert int_list.collect().to_dicts() == [
        {"ints": [0, -1, 1]},
        {"ints": [1, 2, 3]},
    ]


def test_fail_wrong_schema():
    # color should be str, not int
    df = pl.LazyFrame(
        {
            "x": [0, 0, 0],
            "y": [0, -1, 1],
            "color": [0, 1, 2],  # Wrong type: int instead of str
        }
    )
    with pytest.raises(Point.SchemaError):
        Point.from_df(df)


def test_fail_missing_column():
    # Test with a schema that has required (non-nullable) columns
    class RequiredPoint(TypedLazyFrame):
        x: Col[int]  # Required, not nullable
        y: Col[int]  # Required, not nullable

    df = pl.LazyFrame({"x": [0, 0, 0]})
    with pytest.raises(RequiredPoint.SchemaError):
        RequiredPoint.from_df(df)


def test_inheritance():
    df = pl.LazyFrame(
        {
            "x": [0, 0, 0],
            "y": [0, -1, 1],
            "z": [0, -1, 1],
            "color": ["black", "blue", "red"],
        }
    )
    point3d = Point3D.from_df(df)
    assert point3d.collect().to_dicts() == [
        {"x": 0, "y": 0, "z": 0, "color": "black"},
        {"x": 0, "y": -1, "z": -1, "color": "blue"},
        {"x": 0, "y": 1, "z": 1, "color": "red"},
    ]


def test_multiple_inheritance():
    df = pl.LazyFrame(
        {
            "x": [0, 0, 0],
            "y": [0, -1, 1],
            "z": [0, -1, 1],
            "t": [0, 1, 2],
            "color": ["black", "blue", "red"],
            "fillcolor": ["white", "white", "white"],
        }
    )
    point4d = Point4D.from_df(df)
    result = point4d.collect().to_dicts()
    assert len(result) == 3
    assert result[0] == {
        "x": 0,
        "y": 0,
        "z": 0,
        "t": 0,
        "color": "black",
        "fillcolor": "white",
    }


def test_from_dicts():
    data = [
        {"x": 0, "y": 0, "color": "black"},
        {"x": 0, "y": -1, "color": "blue"},
        {"x": 0, "y": 1, "color": "red"},
    ]
    schema = {"x": pl.Int64, "y": pl.Int64, "color": pl.String}
    point = Point.from_dicts(data, schema)
    assert point.collect().to_dicts() == data


def test_validation_can_be_skipped():
    # Test that validation can be disabled
    df = pl.LazyFrame(
        {
            "x": [0, 0, 0],
            "y": [0, -1, 1],
            "color": [0, 1, 2],  # Wrong type, but validation is disabled
        }
    )
    # Should not raise when validate=False
    point = Point.from_df(df, validate=False)
    assert point is not None
