from dataclasses import dataclass
from datetime import date, datetime

import pyarrow as pa
from pandas import ArrowDtype as Dtype

from tableclasses.dc import resolve_type
from tableclasses.errs import UnsupportedTypeError
from tableclasses.pandas.gen import types

from .utils import not_caught


@dataclass
class Test:
    alias: str
    typ: type
    autoresolves: bool
    expected: Dtype


TESTS = [
    Test("bool", bool, True, Dtype(pa.bool_())),
    Test("bytes", bytes, True, Dtype(pa.binary())),
    Test("byte", bytes, False, Dtype(pa.uint8())),
    Test("uint", int, False, Dtype(pa.uint32())),
    Test("uint8", int, False, Dtype(pa.uint8())),
    Test("uint16", int, False, Dtype(pa.uint16())),
    Test("uint32", int, False, Dtype(pa.uint32())),
    Test("uint64", int, False, Dtype(pa.uint64())),
    Test("int", int, True, Dtype(pa.int32())),
    Test("int16", int, False, Dtype(pa.int16())),
    Test("int32", int, True, Dtype(pa.int32())),
    Test("int64", int, False, Dtype(pa.int64())),
    Test("float", float, False, Dtype(pa.float64())),
    Test("float16", float, False, Dtype(pa.float16())),
    Test("float32", float, False, Dtype(pa.float32())),
    Test("float64", float, False, Dtype(pa.float64())),
    Test("datetime", datetime, True, Dtype(pa.date64())),
    Test("date", date, True, Dtype(pa.date32())),
]


def test_resolve_dtype():
    for test in TESTS:
        dt = resolve_type(types, test.typ, test.alias)
        assert dt == test.expected

        if test.autoresolves:
            dt = resolve_type(types, test.typ, "")
            assert dt == test.expected


def test_raises():
    class Custom:
        pass

    type_tests = [
        Custom,
    ]

    for test in type_tests:
        try:
            resolve_type(types, test, "")
            not_caught()
        except RuntimeError as e:
            raise e
        except UnsupportedTypeError:
            pass

    alias_tests = [
        "abcdef",
    ]

    for test in alias_tests:
        try:
            resolve_type(types, test, test)
            not_caught()
        except RuntimeError as e:
            raise e
        except UnsupportedTypeError:
            pass
