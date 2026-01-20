from das.engine.duckdb.typed_relation import Col, TypedRelation


class Point(TypedRelation):
    x: Col[int | None]
    y: Col[int | None]
    color: Col[str | None]


class Point3D(Point):
    z: Col[int | None]


class FillColor(TypedRelation):
    fillcolor: Col[str | None]


class Point4D(Point3D, FillColor):
    t: Col[int | None]


class IntList(TypedRelation):
    ints: Col[list[int]]


def test_as_duckdb_relation():
    point = Point.from_dicts(
        [
            {"x": 0, "y": 0, "color": "black"},
            {"x": 0, "y": -1, "color": "blue"},
            {"x": 0, "y": 1, "color": "red"},
        ]
    )
    assert point.select(Point.color).fetchdf().to_dict(orient="records") == [
        {"color": "black"},
        {"color": "blue"},
        {"color": "red"},
    ]


def test_self_join():
    point1 = Point.from_dicts(
        [
            {"x": 0, "y": 0, "color": "black"},
            {"x": 0, "y": -1, "color": "blue"},
            {"x": 0, "y": 1, "color": "red"},
        ]
    )
    point2 = Point.from_dicts(
        [
            {"x": 0, "y": 0, "color": "black"},
            {"x": 0, "y": -1, "color": "blue"},
            {"x": 0, "y": 1, "color": "red"},
        ]
    )
    joined = point1.join(point2, "x", how="inner")
    assert joined.count("*").fetchall() == [(3 * 3,)]
