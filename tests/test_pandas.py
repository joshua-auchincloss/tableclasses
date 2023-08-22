from datetime import date, datetime
from os import environ
from random import randbytes, randint, random
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
from beartype.roar import BeartypeCallHintParamViolation
from pandas import ArrowDtype as Dtype

from tableclasses.base.field import field
from tableclasses.errs import ColumnError, DataError, RowError
from tableclasses.pandas import tabled

from .utils import get_data, get_row_dicts, not_caught, rename_col_ord


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


@tabled
class TestAliased:
    a: int = field("int32", aliases=["f", "g"])
    b: str = field("string")
    c: float = field("float64")
    d: datetime = field("datetime")
    e: date = field("date")


def get_expect():
    data = get_data()
    s1 = pd.Series(data.get("a")).astype(Dtype(pa.int32()))
    s2 = pd.Series(data.get("b")).astype(Dtype(pa.string()))
    s3 = pd.Series(data.get("c")).astype(Dtype(pa.float64()))
    s4 = pd.Series(data.get("d")).astype(Dtype(pa.date64()))
    s5 = pd.Series(data.get("e")).astype(Dtype(pa.date32()))
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
    t = t.from_existing(t)
    deep_equals(expect, t)

    t = TestUnfielded.from_columns(get_data())
    deep_equals(expect, t)
    t = t.from_existing(t)
    deep_equals(expect, t)

    t = TestFielded.from_existing(pd.DataFrame(get_data()))
    deep_equals(expect, t)
    t = t.from_existing(t)
    deep_equals(expect, t)

    t = TestFielded.from_columns(get_data())
    deep_equals(expect, t)
    t = t.from_existing(t)
    deep_equals(expect, t)

    t = TestTableWrap.from_columns(get_data())
    deep_equals(expect, t)
    t = t.from_existing(t)
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

    t = t.from_existing(t)
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
    t = t.from_existing(pd.DataFrame(data))
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


def test_downstream_series_typing():
    data = get_data()
    test = pd.DataFrame(data)
    t = TestFielded.from_existing(test)
    assert t.a.sum() == 6


def test_aliased():
    expect = get_expect()
    data = get_data()
    test = pd.DataFrame(data)
    test.rename({"a": "f"}, axis=1, inplace=True)
    t = TestAliased.from_existing(test)
    deep_equals(expect, t)

    test_dict = rename_col_ord(data, a="f")
    t = TestAliased.from_columns(test_dict)
    deep_equals(expect, t)

    test_dict = rename_col_ord(data, a="g")
    t = TestAliased.from_columns(test_dict)
    deep_equals(expect, t)


def test_invalid_cols():
    test = get_expect()
    test.rename({"a": "f"}, axis=1, inplace=True)
    try:
        TestFielded.from_existing(test)
        not_caught()
    except RuntimeError as e:
        raise e
    except DataError:
        pass


def test_allowed():
    data = get_data()
    assert TestFielded.allowed() == list(data.keys())
    assert TestUnfielded.allowed() == list(data.keys())


@tabled
class Perf:
    cap: int
    num: int
    std_ms: float
    dur_ms: float


def test_perf():
    tests = [
        100,
        1000,
        10000,
    ]
    max_int = 2147483647
    max_float = 9999999999
    timed = Perf.from_columns({"cap": [], "num": [], "dur_ms": [], "std_ms": []})
    for cap in tests:
        caps = [cap for _ in range(10)]
        nums = list(range(10))
        durs = []
        stds = []
        for _ in nums:
            col_a = [randint(0, max_int) for _ in range(cap)]  # noqa: S311
            col_b = [str(randbytes(randint(0, 240))) for _ in range(cap)]  # noqa: S311
            col_c = [float(randint(0, max_float) + random()) for _ in range(cap)]  # noqa: S311
            col_d = [datetime.fromisoformat(f"2023-01-0{randint(1,9)}") for _ in range(cap)]  # noqa: S311
            col_e = [d.date() for d in col_d]
            data = lambda: {  # noqa: E731
                "a": col_a,  # noqa: B023
                "b": col_b,  # noqa: B023
                "c": col_c,  # noqa: B023
                "d": col_d,  # noqa: B023
                "e": col_e,  # noqa: B023
            }
            start = datetime.now()  # noqa: DTZ005
            d = TestFielded.from_columns(data())
            end = datetime.now()  # noqa: DTZ005
            dur = (end - start).microseconds
            durs.append(dur)
            del d

            start = datetime.now()  # noqa: DTZ005
            d = pd.DataFrame(data())
            end = datetime.now()  # noqa: DTZ005
            dur = (end - start).microseconds
            stds.append(dur)
            del d

        results = Perf.from_columns(
            {
                "cap": caps,
                "num": nums,
                "std_ms": stds,
                "dur_ms": durs,
            }
        )
        timed = pd.concat((timed, results), axis=0)

    if environ.get("WRITE_REPORT"):
        with open("perf/timed-pandas.md", "w") as f:
            timed.to_markdown(f)
    else:
        print(timed.to_markdown())  # noqa: T201
