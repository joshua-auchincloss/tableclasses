from typing import Callable

from tableclasses.base.field import FieldMeta
from tableclasses.errs import ColumnError, GetRepr, RowError
from tableclasses.types import CellValueGetter, ColumnLike, Indexable, RowValidator


def resolve_row_fs(row: any, name: str, allow_positional: bool) -> tuple[CellValueGetter, RowValidator]:
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


def must_get_col(
    getter: Callable[[any, str, FieldMeta, GetRepr], ColumnLike],
    source: any,
    meta: FieldMeta,
    get_repr: GetRepr,
) -> ColumnLike:
    col = None
    try:
        col = getter(source, meta.col_name, meta, get_repr)
    except ColumnError as e:
        for name in meta.aliases:
            try:
                col = getter(source, name, meta, get_repr)
            except ColumnError:
                pass
        if col is None:
            raise e
    return col


def get_column(
    named_cols: dict[str, ColumnLike],
    name: str,
    meta: FieldMeta,
    get_repr: GetRepr,
) -> ColumnLike:
    col = named_cols.get(name)
    if col is None:
        raise ColumnError(meta, get_repr, col)
    return col


def get_keyed(
    other: Indexable,
    name: str,
    meta: FieldMeta,
    get_repr: GetRepr,
):
    try:
        return other[name]
    except KeyError as e:
        raise ColumnError(meta, get_repr, other.columns) from e
