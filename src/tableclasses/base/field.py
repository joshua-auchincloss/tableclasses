from dataclasses import asdict, dataclass
from dataclasses import field as _field
from typing import Optional

from pandas import ArrowDtype

from tableclasses.types import P


@dataclass
class FieldMeta:
    typ: str
    index: bool
    col_name: Optional[str] = None
    arrow: Optional[ArrowDtype] = None


def field(
    typ: str,
    *args: P.args,
    index: Optional[bool] = False,
    col_name: Optional[str] = None,
    **kwargs: P.kwargs,
):
    meta = kwargs.get("metadata")
    if meta is None:
        meta = {}
    modelled = FieldMeta(
        typ=typ,
        index=index,
        col_name=col_name,
        arrow=None,
    )
    meta = {**meta, **asdict(modelled)}
    return _field(*args, **kwargs, metadata=meta)
