from copy import deepcopy
from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
from beartype.roar import BeartypeCallHintParamViolation
from pandas import ArrowDtype as dtype

from tableclasses.base.field import field
from tableclasses.base.tabled import Base
from tableclasses.errs import RowError, ColumnError, DataError
from tableclasses.pandas.tabled import DataFrame
from tableclasses.pandas import tabled

from .utils import not_caught

dates = [
    "2023-08-01",
    "2023-08-02",
    "2023-08-03",
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

@tabled
class TestUnfielded:
    a: int
    b: str
    c: float
    d: datetime
    e: date


@tabled
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


@tabled
class TestIndexed:
    a: int = field("int32", index=True)
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

    tests = [
        None,
        "A",
    ]
    for test in tests:
        try:
            TestUnfielded.from_rows(test)
            not_caught()
        except RuntimeError as e:
            raise e
        except BeartypeCallHintParamViolation:
            pass


def test_column_errs():
    b1 = get_data()
    b1.pop("e")
    muts = [
        b1,
        {**get_data(), "extra": [1]},
    ]

    for mut in muts:
        try:
            TestFielded.from_columns(mut)
            not_caught()
        except RuntimeError as e:
            raise e
        except (ColumnError, DataError):
            pass

def test_indexed():
    data = get_data()
    expect = get_expect().set_index("a")

    t = TestIndexed.from_columns(data)
    deep_equals(expect, t)

    t = TestIndexed.from_existing(pd.DataFrame(data))
    deep_equals(expect, t)

    row_dicts = get_row_dicts()
    t = TestIndexed.from_rows(row_dicts)
    deep_equals(expect, t)

def test_invalid_rows():
    row_dicts = get_row_dicts()
    row_tup1 = []
    for row in row_dicts:
        row_tup1.append(tuple(row.values()))

    row_tup2 = []
    for row in row_dicts:
        # extra value
        row_tup2.append((*row.values(), 2))


    row_dict = []
    for row in row_dicts:
        row_dict.append({**row, "extra": 1})

    tests = [
        (row_tup1, False),
        (row_tup2, True),
        (row_dict, True),
    ]
    for rows, allow in tests:
        try:
            TestFielded.from_rows(rows, allow_positional=allow)
            not_caught()
        except RuntimeError as e:
            raise e
        except RowError:
            pass


def test_invalid_cols():
    test = get_expect()
    test.rename({"a": "f"}, axis=1, inplace=True)
    try:
        TestFielded.from_existing(test)
        not_caught()
    except RuntimeError as e:
        raise e
    except ColumnError:
        pass


def test_allowed():
    assert TestFielded.allowed() == list(DATA.keys())
    assert TestUnfielded.allowed() == list(DATA.keys())