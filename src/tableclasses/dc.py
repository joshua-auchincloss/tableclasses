from dataclasses import Field, asdict, dataclass, fields, is_dataclass
from datetime import date, datetime
from typing import Callable, Dict, TypeVar

import pyarrow as pa

from tableclasses.base.field import FieldMeta
from tableclasses.base.tabled import Wrapped
from tableclasses.errs import UnsupportedTypeError
from tableclasses.types import TableType, TypeDict, TypeKey

T = TypeVar("T")


@dataclass
class TypeMapping:
    arrow: Callable[[], any]


types: Dict[TypeKey, TypeMapping] = {
    # strings
    str: TypeMapping(pa.string),
    "string": TypeMapping(pa.string),
    "str": TypeMapping(pa.string),
    # booleans
    bool: TypeMapping(pa.bool_),
    "bool": TypeMapping(pa.bool_),
    # binary
    bytes: TypeMapping(pa.binary),
    "bytes": TypeMapping(pa.binary),
    "byte": TypeMapping(pa.uint8),
    # uint
    "uint": TypeMapping(pa.uint32),
    "uint8": TypeMapping(pa.uint8),
    "uint16": TypeMapping(pa.uint16),
    "uint32": TypeMapping(pa.uint32),
    "uint64": TypeMapping(pa.uint64),
    # int
    int: TypeMapping(pa.int32),
    "int": TypeMapping(pa.int32),
    "int8": TypeMapping(pa.int8),
    "int16": TypeMapping(pa.int16),
    "int32": TypeMapping(pa.int32),
    "int64": TypeMapping(pa.int64),
    # float / double
    float: TypeMapping(pa.float64),
    "float": TypeMapping(pa.float64),
    "float16": TypeMapping(pa.float16),
    "float32": TypeMapping(pa.float32),
    "float64": TypeMapping(pa.float64),
    # dates
    date: TypeMapping(pa.date32),
    "date": TypeMapping(pa.date32),
    datetime: TypeMapping(pa.date64),
    "datetime": TypeMapping(pa.date64),
}


def resolve_type(types: TypeDict, native: type, alias: str) -> TableType:
    if alias == "":
        typ = types.get(native)
    else:
        typ = types.get(alias)

    # dont check twice
    if typ is None and alias != "":
        typ = types.get(native)

    if typ is None:
        raise UnsupportedTypeError(native, alias)
    return typ


def gen(cls: T, with_known: Callable[[list[Field], T], Wrapped], type_mapping: TypeDict) -> Wrapped:
    orig = cls
    if not is_dataclass(orig):
        cls = dataclass(orig)

    pre = fields(cls)
    known = []
    for field in pre:
        if len(field.metadata) > 0:
            meta = FieldMeta(**field.metadata)
        else:
            meta = FieldMeta("", col_name=field.name)
        dtype = resolve_type(types=type_mapping, native=field.type, alias=meta.typ)
        meta.arrow = dtype
        if meta.col_name is None:
            meta.col_name = field.name
        field.metadata = asdict(meta)
        known.append(field)
    return with_known(known, orig)
