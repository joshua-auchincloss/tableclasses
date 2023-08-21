from dataclasses import Field
from typing import Dict, Generic, List, overload

from tableclasses.base.field import FieldMeta
from tableclasses.types import Cls, ColumnLike, RowLike, Tabular


class Base(
    Generic[
        Cls,
        Tabular,
    ]
):
    __known__ = List[Field]

    @overload
    def __init__(self, **named_cols: Dict[str, ColumnLike]):
        ...

    @overload
    @classmethod
    def from_existing(cls, other: Tabular) -> Tabular:  # pragma: no cover
        ...

    @overload
    @classmethod
    def from_columns(cls, **named_cols: Dict[str, ColumnLike]) -> Tabular:  # pragma: no cover
        ...

    @overload
    @classmethod
    def from_rows(cls, rows: RowLike) -> Tabular:  # pragma: no cover
        ...

    @classmethod
    def allowed(cls) -> List[str]:
        fields = []
        for field in cls.__known__:
            if len(field.metadata) > 0:
                meta = FieldMeta(**field.metadata)
                fields.append(meta.col_name)
            else:
                fields.append(field.name)
        return fields