from typing import Annotated, Generic

from beartype import beartype
from beartype.vale import Is
from pyarrow import Table as _Table
from pyarrow import field as _field
from pyarrow import schema as _schema

from tableclasses.base.field import FieldMeta
from tableclasses.base.tabled import Base
from tableclasses.errs import ColumnError
from tableclasses.pandas.tabled import ColumnArgs, get_column, must_get_col, valid_cols
from tableclasses.types import Cls


def allowed_repr(meta: FieldMeta):
    typ = meta.typ
    return f"(pa.Array[{typ}] | list[{typ}] | Generator[{typ}])"


def get_df(other: _Table, name: str, meta: FieldMeta):
    col = other.column(name)
    if col is None:
        raise ColumnError(meta, allowed_repr, col)
    return col


class Table(Generic[Cls], Base[Cls, _Table], _Table):
    @beartype
    @classmethod
    def from_columns(cls, named_cols: Annotated[dict[str, ColumnArgs], Is[valid_cols]]):
        cols = []
        schemas = []
        cls.validate_allowed(named_cols.keys())
        for field in cls.__known__:
            meta = FieldMeta(**field.metadata)
            cols.append(must_get_col(get_column, named_cols, meta))
            schemas.append(_field(meta.col_name, meta.arrow()))
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
            col = must_get_col(get_df, other, meta)
            cols.append(col)
            schemas.append(_field(meta.col_name, meta.arrow()))

        return _Table.from_arrays(
            cols,
            schema=_schema(
                schemas,
            ),
        )
