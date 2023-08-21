from collections.abc import Generator
from typing import Annotated, Generic, TypeVar

from beartype import beartype
from beartype.vale import Is
from pyarrow import Array as _Array
from pyarrow import Table as _Table
from pyarrow import field as _field
from pyarrow import schema as _schema

from tableclasses.base.field import FieldMeta
from tableclasses.base.tabled import Base
from tableclasses.errs import ColumnError, GetRepr
from tableclasses.pandas.tabled import get_column, must_get_col
from tableclasses.types import Cls

ColumnArgs = TypeVar("ColumnArgs", _Array, list, Generator)
NamedColumns = dict[str, ColumnArgs]


def allowed_repr(meta: FieldMeta):
    typ = meta.typ
    return f"(pa.Array[{typ}] | list[{typ}] | Generator[{typ}])"


def get_table(other: _Table, name: str, meta: FieldMeta, get_repr: GetRepr):
    col = other.column(name)
    if col is None:
        raise ColumnError(meta, get_repr, col)
    return col


def valid_cols(cols: NamedColumns):
    for col in cols.values():
        if not isinstance(col, (_Array, Generator, list)):
            return False
    return True


class Table(Generic[Cls], Base[Cls, _Table], _Table):
    @beartype
    @classmethod
    def from_columns(cls, columns: Annotated[NamedColumns, Is[valid_cols]]):
        cols = []
        schemas = []
        cls.validate_allowed(columns.keys())
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            cols.append(must_get_col(get_column, columns, meta, allowed_repr))
            schemas.append(_field(meta.col_name, meta.arrow))
        return _Table.from_arrays(
            cols,
            schema=_schema(
                schemas,
            ),
        )

    @beartype
    @classmethod
    def from_existing(cls, other: _Table):
        cols = []
        schemas = []
        cls.validate_allowed([c.name for c in other.schema])
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            col = must_get_col(get_table, other, meta, allowed_repr)
            cols.append(col)
            schemas.append(_field(meta.col_name, meta.arrow))

        return _Table.from_arrays(
            cols,
            schema=_schema(
                schemas,
            ),
        )
