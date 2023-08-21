from typing import Iterable, ParamSpec, TypeVar

Cls = TypeVar("Cls")
T = ParamSpec("T")
P = ParamSpec("P")
Tabular = TypeVar("Tabular")
RowLike = TypeVar("RowLike", bound=Iterable)
ColumnLike = TypeVar("ColumnLike")
