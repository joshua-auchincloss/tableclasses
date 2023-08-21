from dataclasses import Field
from typing import Dict, Generic, List, Optional, TypeVar, overload

from tableclasses.base.field import FieldMeta
from tableclasses.types import Cls, ColumnLike, RowsLike, Tabular


class Base(
    Generic[
        Cls,
        Tabular,
    ]
):
    __known__ = List[Field]

    @overload
    def __init__(self):  # pragma: no cover
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
    def from_rows(
        cls, rows: RowsLike, allow_positional: Optional[bool] = False  # noqa: FBT002
    ) -> Tabular:  # pragma: no cover
        ...

    @classmethod
    def allowed(cls) -> List[str]:
        fields = []
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            fields.append(meta.col_name)
        return fields


Wrapped = TypeVar("Wrapped", bound=Base)
