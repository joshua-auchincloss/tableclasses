from dataclasses import Field, make_dataclass
from typing import Generic, Optional

from pandas import ArrowDtype

from tableclasses.base.field import FieldMeta
from tableclasses.dc import gen, map_types
from tableclasses.pandas.tabled import DataFrame
from tableclasses.types import Cls, P, T

types = map_types(ArrowDtype)


class Series(Generic[T]):
    ...


def with_known(known: list[Field], cls: Cls) -> "DataFrame[Cls]":
    slots: tuple[tuple[str, type], ...] = ()
    for field in known:
        meta = FieldMeta(**field.metadata)
        slots += ((meta.col_name, Series[meta.col_name]),)

    class Wrapped(
        DataFrame[Cls],
    ):
        __known__ = known

    cls = make_dataclass(
        cls.__name__,
        slots,
        bases=(Wrapped,),
        init=False,
    )
    return cls


def wrapper(cls: Optional[Cls] = None, *args: P.args, **kwargs: P.kwargs):
    def _gen(cls: Cls) -> DataFrame[Cls]:
        return gen(cls, *args, with_known=with_known, type_mapping=types, **kwargs)

    return _gen(cls) if cls else _gen
