from typing import ClassVar, TypeVar, get_args, get_origin

import wrapt

T = TypeVar("T")


class ColBase[T]:
    """DataFrame column descriptor."""

    # this will be implicitly set by TypedDataFrameBase subclass definition, via __set_name__
    name: str = ""
    python_type: type[T]

    def __init__(self, python_type: type[T]):
        """Store the Python data type as part of column."""
        self.python_type = python_type

    def __set_name__(self, owner, name: str):
        """Sets the column name as it was set in the class definition."""
        self.name = name


class TypedDataFrameBase(wrapt.ObjectProxy):
    """
    Base class for schema definitions for any DataFrame-like class.
    Wraps and behaves like the underlying DataFrame-like class.

    If inherited with abstract=True kwarg, subclass will behave like TypedDataFrameBase.
    """

    DataFrameModel: ClassVar[type]
    _schema_class: ClassVar[type]

    def __init__(self, df):
        # this instance will be a proxy to the given dataframe
        super().__init__(df)

    def __init_subclass__(cls, abstract: bool = False, **kwargs):
        """For classes that inherit from TypedDataFrame, extract columns from class type annotations."""
        super().__init_subclass__(**kwargs)

        # Don't do anything extra for abstract subclasses
        if abstract:
            return

        # Collect column annotations from all suitable base classes
        all_annotations = {}

        for base in reversed(cls.__mro__):  # from base to derived
            # skip non-relevant base classes
            if base in (object, wrapt.ObjectProxy, TypedDataFrameBase):
                continue

            # Get annotations (use getattr to handle Python 3.14 lazy annotations)
            own_annotations = getattr(base, "__annotations__", {})
            if not isinstance(own_annotations, dict):
                continue

            # Extract columns from class definition
            for attr_name, annotation in own_annotations.items():
                attr_class = get_origin(annotation)
                if (
                    attr_class is not None
                    and isinstance(attr_class, type)
                    and issubclass(attr_class, ColBase)
                ):
                    args = get_args(annotation)
                    col_type = args[0]
                    col = attr_class(col_type)
                    col.name = attr_name
                    all_annotations[attr_name] = col_type
                    setattr(cls, attr_name, col)

        # construct schema class
        cls._schema_class = type(
            f"{cls.__name__}Schema",
            (cls.DataFrameModel,),
            {"__annotations__": all_annotations},
        )
