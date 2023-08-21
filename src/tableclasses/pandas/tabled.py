from collections.abc import Generator
from typing import Annotated, Generic, TypeVar

from beartype import beartype
from beartype.vale import Is
from pandas import DataFrame as _DataFrame
from pandas import Series as _Series

from tableclasses.base.field import FieldMeta
from tableclasses.base.tabled import Base
from tableclasses.base.utils import get_column, get_keyed, must_get_col, resolve_row_fs
from tableclasses.types import Cls, RowsLike

T = TypeVar("T")
ColumnArgs = TypeVar("ColumnArgs", _Series, list, Generator)
NamedColumns = dict[str, ColumnArgs]


def allowed_repr(meta: FieldMeta):
    typ = meta.typ
    return f"(pd.Series[{typ}] | list[{typ}] | Generator[{typ}])"


def valid_cols(cols: NamedColumns):
    for col in cols.values():
        if not isinstance(col, (_Series, Generator, list)):
            return False
    return True


def valid_rows(rows: RowsLike):
    return isinstance(rows, (list, Generator, tuple))


class DataFrame(Generic[Cls], Base[Cls, _DataFrame], _DataFrame):
    @beartype
    @classmethod
    def from_columns(cls, columns: Annotated[NamedColumns, Is[valid_cols]]):
        self = cls.__new__(cls)
        cols = {}
        idx = []
        cls.validate_allowed(columns.keys())
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            col = must_get_col(get_column, columns, meta, allowed_repr)
            cols[meta.col_name] = _Series(col).astype(meta.arrow)
            if meta.index:
                idx.append(meta.col_name)

        _DataFrame.__init__(self, cols)
        if len(idx) > 0:
            self = self.set_index(idx)
        return self

    @beartype
    @classmethod
    def from_existing(cls, other: _DataFrame):
        self = other.copy(False)
        idx = []
        cls.validate_allowed(other.columns)
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            colname = meta.col_name
            col = must_get_col(get_keyed, other, meta, allowed_repr)
            self[colname] = _Series(col).astype(meta.arrow)
            if meta.index:
                idx.append(colname)
        if len(idx) > 0:
            self = self.set_index(idx)
        return self

    @beartype
    @classmethod
    def from_rows(cls, rows: RowsLike, allow_positional: bool = False):  # noqa: FBT002
        self = cls.__new__(cls)
        keyed = {}
        getter = None
        checker = None
        num = len(cls.__known__)
        idx = []
        for i, known in enumerate(cls.__known__):
            meta = FieldMeta(**known.metadata)
            values = []
            for row in rows:
                if getter is None or checker is None:
                    (getter, checker) = resolve_row_fs(row=row, name=meta.col_name, allow_positional=allow_positional)

                checker(row, num)
                value = getter(row, meta.col_name, i)

                values.append(value)

            keyed[meta.col_name] = _Series(
                values,
                dtype=meta.arrow,
            )
            if meta.index:
                idx.append(meta.col_name)
        _DataFrame.__init__(self, keyed)
        if len(idx) > 0:
            self = self.set_index(idx)
        return self
