from collections.abc import Generator
from typing import Annotated, Generic, TypeVar

from beartype import beartype
from beartype.vale import Is
from pandas import DataFrame as _DataFrame
from pandas import Series as _Series

from tableclasses.base.field import FieldMeta
from tableclasses.base.tabled import Base
from tableclasses.errs import ColumnError, DataError, RowError
from tableclasses.types import CellValueGetter, RowsLike, RowValidator

T = TypeVar("T")
ColumnArgs = TypeVar("ColumnArgs", _Series, list, Generator)


def resolve_rowfs(row: any, name: str, allow_positional: bool) -> tuple[CellValueGetter, RowValidator]:
    if hasattr(row, "get") and callable(row.get):

        def getter(row: dict, name: str, _: int):
            return row.get(name)

        def valid(row: dict, n_fields: int):
            if not len(row.keys()) == n_fields:
                raise RowError(row, f"{row.keys()}")

    elif hasattr(row, name):

        def getter(row: any, name: str, _: int):
            return getattr(row, name)

        def valid(_: any, __: int):
            pass

    elif isinstance(row, tuple) and allow_positional:

        def getter(row: tuple, _: str, idx: int):
            try:
                return row[idx]
            except IndexError as e:  # pragma: no cov
                raise RowError(row, "(..., len == fields.length)") from e

        def valid(row: tuple, n_fields: int):
            if not len(row) == n_fields:
                raise RowError(row, f"{row}")

    else:
        raise RowError(row, "(list, iterable, object)")
    return getter, valid


def allowed_repr(meta: FieldMeta):
    typ = meta.typ
    return f"(pd.Series[{typ}] | list[{typ}] | Generator[{typ}])"


def valid_cols(cols: dict[str, ColumnArgs]):
    for col in cols.values():
        if not isinstance(col, (_Series, Generator, list)):
            return False
    return True


def valid_rows(rows: RowsLike):
    return isinstance(rows, (list, Generator, tuple))


class DataFrame(Generic[T], Base[T, _DataFrame], _DataFrame):
    @beartype
    @classmethod
    def from_columns(cls, named_cols: Annotated[dict[str, ColumnArgs], Is[valid_cols]]):
        self = cls.__new__(cls)
        cols = {}
        allowed = cls.allowed()
        disallowed = [field for field in named_cols.keys() if field not in allowed]
        if len(disallowed) > 0:
            err = "({:}, ...) is unknown to the model".format(",".join(disallowed))
            raise DataError(err)
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            colname = meta.col_name
            col = named_cols.get(colname)
            if col is None:
                raise ColumnError(meta, allowed_repr, col)
            cols[colname] = _Series(col).astype(meta.arrow)
        _DataFrame.__init__(self, cols)
        return self

    @beartype
    @classmethod
    def from_existing(cls, other: _DataFrame):
        self = other.copy(False)
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            colname = meta.col_name
            try:
                col = other[colname]
            except KeyError as e:
                raise ColumnError(meta, allowed_repr, None) from e
            self[colname] = _Series(col).astype(meta.arrow)
        return self

    @beartype
    @classmethod
    def from_rows(cls, rows: RowsLike, allow_positional: bool = False):  # noqa: FBT002
        self = cls.__new__(cls)
        keyed = {}
        getter = None
        checker = None
        num = len(cls.__known__)
        for i, known in enumerate(cls.__known__):
            meta = FieldMeta(**known.metadata)
            values = []
            for row in rows:
                if getter is None or checker is None:
                    (getter, checker) = resolve_rowfs(row=row, name=meta.col_name, allow_positional=allow_positional)

                checker(row, num)
                value = getter(row, meta.col_name, i)

                values.append(value)
            keyed[meta.col_name] = _Series(
                values,
                dtype=meta.arrow,
            )
        _DataFrame.__init__(self, keyed)
        return self
