from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd
import pyarrow as pa
from pandas import ArrowDtype as dtype

from tableclasses.dc import resolve_type
from tableclasses.pandas.gen import types
from tableclasses.errs import UnsupportedTypeError

from .utils import not_caught


@dataclass
class Test:
    alias: str
    typ: type
    autoresolves: bool
    expected: dtype


TESTS = [
    Test("bool", bool, True, dtype(pa.bool_())),
    Test("bytes", bytes, True, dtype(pa.binary())),
    Test("byte", bytes, False, dtype(pa.uint8())),
    Test("uint", int, False, dtype(pa.uint32())),
    Test("uint8", int, False, dtype(pa.uint8())),
    Test("uint16", int, False, dtype(pa.uint16())),
    Test("uint32", int, False, dtype(pa.uint32())),
    Test("uint64", int, False, dtype(pa.uint64())),
    Test("int", int, True, dtype(pa.int32())),
    Test("int16", int, False, dtype(pa.int16())),
    Test("int32", int, True, dtype(pa.int32())),
    Test("int64", int, False, dtype(pa.int64())),
    Test("float", float, False, dtype(pa.float64())),
    Test("float16", float, False, dtype(pa.float16())),
    Test("float32", float, False, dtype(pa.float32())),
    Test("float64", float, False, dtype(pa.float64())),
    Test("datetime", datetime, True, dtype(pa.date64())),
    Test("date", date, True, dtype(pa.date32())),
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
