from collections.abc import Generator
from typing import Callable, Dict, Generic, ParamSpec, Protocol, TypeVar

T = TypeVar("T")
P = ParamSpec("P")
Cls = TypeVar("Cls")
Tabular = TypeVar("Tabular")
RowsLike = TypeVar(
    "RowsLike",
    list,
    Generator,
    tuple,
)
ColumnLike = TypeVar("ColumnLike")
ArrowType = TypeVar("ArrowType")
TableType = TypeVar("TableType")
TypeKey = TypeVar("TypeKey", str, type)
TypeDict = Dict[TypeKey, TableType]
RowValidator = Callable[[any, int], None]
CellValue = TypeVar("CellValue")
CellValueGetter = Callable[[any, str, int], CellValue]


class Indexable(Protocol, Generic[T]):
    def __getitem__(self, key: int) -> T:  # pragma: no cov
        ...
