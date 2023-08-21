from typing import Callable, Optional, TypeVar

from tableclasses._dc import gen
from tableclasses.pandas import DataFrame
from tableclasses.types import P

T = TypeVar("T")
Dc = TypeVar("Dc", bound=DataFrame)


def tabled(cls: Optional[T] = None, *args: P.args, **kwargs: P.kwargs) -> Callable[[T], Dc] | Dc:
    def inner(cls: T) -> Dc:
        return gen(cls, *args, **kwargs)

    return inner(cls) if cls else inner
