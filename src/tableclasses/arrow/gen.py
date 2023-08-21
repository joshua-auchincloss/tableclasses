from dataclasses import Field
from typing import Optional

from tableclasses.arrow.tabled import Table
from tableclasses.dc import gen, types
from tableclasses.types import Cls, P


def with_known(known: list[Field], orig: Cls) -> "Table[Cls]":
    class Wrapped(
        Table[orig],
    ):
        __known__ = known

    return Wrapped


def wrapper(cls: Optional[Cls] = None, *args: P.args, **kwargs: P.kwargs):
    def _gen(cls: Cls) -> Table[Cls]:
        return gen(cls, *args, with_known=with_known, type_mapping=types, **kwargs)

    return _gen(cls) if cls else _gen
