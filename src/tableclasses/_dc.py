from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import date, datetime
from typing import TypeVar

import pyarrow as pa
from pandas import ArrowDtype

from tableclasses.base.errs import UnsupportedTypeError
from tableclasses.base.field import FieldMeta
from tableclasses.pandas.table import DataFrame

T = TypeVar("T")

types = {
    # strings
    str: ArrowDtype(pa.string()),
    "string": ArrowDtype(pa.string()),
    "str": ArrowDtype(pa.string()),
    # booleans
    bool: ArrowDtype(pa.bool_()),
    "bool": ArrowDtype(pa.bool_()),
    # binary
    bytes: ArrowDtype(pa.binary()),
    "bytes": ArrowDtype(pa.binary()),
    "byte": ArrowDtype(pa.uint8()),
    # uint
    "uint": ArrowDtype(pa.uint32()),
    "uint8": ArrowDtype(pa.uint8()),
    "uint16": ArrowDtype(pa.uint16()),
    "uint32": ArrowDtype(pa.uint32()),
    "uint64": ArrowDtype(pa.uint64()),
    # int
    int: ArrowDtype(pa.int32()),
    "int": ArrowDtype(pa.int32()),
    "int16": ArrowDtype(pa.int16()),
    "int32": ArrowDtype(pa.int32()),
    "int64": ArrowDtype(pa.int64()),
    # float / double
    float: ArrowDtype(pa.float64()),
    "float": ArrowDtype(pa.float64()),
    "float16": ArrowDtype(pa.float16()),
    "float32": ArrowDtype(pa.float32()),
    "float64": ArrowDtype(pa.float64()),
    # dates
    date: ArrowDtype(pa.date32()),
    "date": ArrowDtype(pa.date32()),
    datetime: ArrowDtype(pa.date64()),
    "datetime": ArrowDtype(pa.date64()),
}


def resolve_type(native: type, alias: str) -> ArrowDtype:
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


def gen(cls: T) -> DataFrame[T]:
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
        dtype = resolve_type(field.type, meta.typ)
        meta.arrow = dtype
        if meta.col_name is None:
            meta.col_name = field.name
        field.metadata = asdict(meta)
        known.append(field)

    class Wrapped(
        DataFrame[orig],
    ):
        __known__ = known

    return Wrapped
