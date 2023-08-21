from dataclasses import Field, asdict, dataclass, fields, is_dataclass
from datetime import date, datetime
from typing import Callable, Dict, TypeVar

import pyarrow as pa

from tableclasses.base.field import FieldMeta
from tableclasses.base.tabled import Wrapped
from tableclasses.errs import UnsupportedTypeError
from tableclasses.types import ArrowType, Cls, TableType, TypeDict, TypeKey

T = TypeVar("T")


types: Dict[TypeKey, Callable[[], ArrowType]] = {
    # strings
    str: pa.string,
    "string": pa.string,
    "str": pa.string,
    # booleans
    bool: pa.bool_,
    "bool": pa.bool_,
    # binary
    bytes: pa.binary,
    "bytes": pa.binary,
    "byte": pa.uint8,
    # uint
    "uint": pa.uint32,
    "uint8": pa.uint8,
    "uint16": pa.uint16,
    "uint32": pa.uint32,
    "uint64": pa.uint64,
    # int
    int: pa.int32,
    "int": pa.int32,
    "int8": pa.int8,
    "int16": pa.int16,
    "int32": pa.int32,
    "int64": pa.int64,
    # float / double
    float: pa.float64,
    "float": pa.float64,
    "float16": pa.float16,
    "float32": pa.float32,
    "float64": pa.float64,
    # dates
    date: pa.date32,
    "date": pa.date32,
    datetime: pa.date64,
    "datetime": pa.date64,
}


def map_types(func: Callable[[ArrowType], TableType]) -> TypeDict:
    return {kind: func(arrow_type()) for kind, arrow_type in types.items()}


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


def gen(cls: Cls, with_known: Callable[[list[Field], Cls], Wrapped], type_mapping: TypeDict) -> Wrapped:
    orig = cls
    if not is_dataclass(orig):
        cls = dataclass(orig)

    pre = fields(cls)
    known = []
    for field in pre:
        if len(field.metadata) > 0:
            meta = FieldMeta(**field.metadata)
        else:
            meta = FieldMeta("", col_name=field.name, index=False, aliases=[])

        meta.arrow = resolve_type(types=type_mapping, native=field.type, alias=meta.typ)
        if meta.col_name is None:
            meta.col_name = field.name
        field.metadata = asdict(meta)
        known.append(field)
    return with_known(known, orig)
