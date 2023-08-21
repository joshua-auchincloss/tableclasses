from copy import deepcopy
from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
from beartype.roar import BeartypeCallHintParamViolation
from pandas import ArrowDtype as dtype

from tableclasses import tabled
from tableclasses._dc import gen
from tableclasses.base.field import field
from tableclasses.base.tabled import Base
from tableclasses.base.errs import RowError
from tableclasses.pandas.table import DataFrame

from .utils import not_caught

dates = [
    "2023-08-01T01:14:04Z",
    "2023-08-02T01:14:04Z",
    "2023-08-03T01:14:04Z",
]
DATES = [datetime.fromisoformat(d) for d in dates]

DATA = {
    "a": [1, 2, 3],
    "b": ["a", "b", "c"],
    "c": [1.0, 2.0, 3.0],
    "d": DATES,
    "e": [d.date() for d in DATES],
}


def get_data():
    return deepcopy(DATA)


@gen
class TestUnfielded:
    a: int
    b: str
    c: float
    d: datetime
    e: date


@gen
class TestFielded:
    a: int = field("int32")
    b: str = field("string")
    c: float = field("float64")
    d: datetime = field("datetime")
    e: date = field("date")


@tabled()
class TestTableWrap:
    a: int = field("int32")
    b: str = field("string")
    c: float = field("float64")
    d: datetime = field("datetime")
    e: date = field("date")

def get_row_dicts():
    row_dicts = [{} for v in DATA.get("a")]
    for key, val in DATA.items():
        for i, v in enumerate(val):
            if len(row_dicts) < (i - 1):
                row_dicts.append({})
            row_dicts[i][key] = v
    return row_dicts

def get_expect():
    data = get_data()
    s1 = pd.Series(data.get("a")).astype(dtype(pa.int32()))
    s2 = pd.Series(data.get("b")).astype(dtype(pa.string()))
    s3 = pd.Series(data.get("c")).astype(dtype(pa.float64()))
    s4 = pd.Series(data.get("d")).astype(dtype(pa.date64()))
    s5 = pd.Series(data.get("e")).astype(dtype(pa.date32()))
    return pd.DataFrame(
        {
            "a": s1,
            "b": s2,
            "c": s3,
            "d": s4,
            "e": s5,
        }
    )


def deep_equals(ground: pd.DataFrame, test: pd.DataFrame):
    for col in ground.columns:
        gc = ground[col]
        tc = test[col]
        assert gc.equals(tc)
        assert gc.dtype == tc.dtype


def test_base():
    expect = get_expect()
    t = TestUnfielded.from_existing(pd.DataFrame(get_data()))
    deep_equals(expect, t)
    t = TestUnfielded.from_columns(get_data())
    deep_equals(expect, t)

    t = TestFielded.from_existing(pd.DataFrame(get_data()))
    deep_equals(expect, t)

    t = TestFielded.from_columns(get_data())
    deep_equals(expect, t)

    t = TestTableWrap.from_columns(get_data())
    deep_equals(expect, t)


def test_row_order():
    expect = get_expect()
    row_dicts = get_row_dicts()

    t = TestFielded.from_rows(row_dicts)
    deep_equals(expect, t)

    row_ns = []
    for row in row_dicts:
        row_ns.append(SimpleNamespace(**row))

    t = TestFielded.from_rows(row_ns)
    deep_equals(expect, t)

    row_tup = []
    for row in row_dicts:
        row_tup.append(tuple(row.values()))
    t = TestFielded.from_rows(row_tup, allow_positional=True)
    deep_equals(expect, t)


def test_raises_invalid_type():
    tests = [{"a": 1}, {"a": {1, 2, 3}}, {"a": None}]
    for test in tests:
        try:
            TestUnfielded.from_columns(test)
            not_caught()
        except RuntimeError as e:
            raise e
        except BeartypeCallHintParamViolation:
            pass


def test_invalid_rows():
    row_dicts = get_row_dicts()
    row_tup = []
    for row in row_dicts:
        row_tup.append(tuple(row.values()))
    try:
        TestFielded.from_rows(row_tup, allow_positional=False)
        not_caught()
    except RuntimeError as e:
        raise e
    except RowError:
        pass