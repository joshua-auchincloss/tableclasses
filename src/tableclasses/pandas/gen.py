from dataclasses import Field
from typing import Optional

from pandas import ArrowDtype

from tableclasses.dc import gen, types
from tableclasses.pandas.tabled import DataFrame
from tableclasses.types import Cls, P

types = {k: ArrowDtype(v.arrow()) for k, v in types.items()}


def wrapper(cls: Optional[Cls] = None, *args: P.args, **kwargs: P.kwargs):
    def with_known(known: list[Field], orig: Cls):
        class Wrapped(
            DataFrame[orig],
        ):
            __known__ = known

        return Wrapped

    def _gen(cls: Cls) -> DataFrame[Cls]:
        return gen(cls, *args, with_known=with_known, type_mapping=types, **kwargs)

    return _gen(cls) if cls else _gen
