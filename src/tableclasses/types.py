from collections.abc import Generator
from typing import Callable, Dict, ParamSpec, TypeVar

T = ParamSpec("T")
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
