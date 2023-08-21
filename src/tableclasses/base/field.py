from dataclasses import asdict, dataclass
from dataclasses import field as _field
from typing import Optional

from tableclasses.types import P, TableType


@dataclass
class FieldMeta:
    typ: str
    index: bool
    aliases: list[str]
    col_name: Optional[str] = None
    arrow: Optional[TableType] = None


def field(
    typ: str,
    *args: P.args,
    index: Optional[bool] = False,
    aliases: Optional[list[str]] = None,
    col_name: Optional[str] = None,
    **kwargs: P.kwargs,
):
    meta = kwargs.get("metadata")
    if meta is None:
        meta = {}
    if aliases is None:
        aliases = []
    modelled = FieldMeta(typ=typ, index=index, col_name=col_name, arrow=None, aliases=aliases)
    meta = {**meta, **asdict(modelled)}
    return _field(*args, **kwargs, metadata=meta)
