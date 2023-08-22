from datetime import date, datetime

import pyarrow as pa

from tableclasses.arrow import tabled
from tableclasses.base.field import field

from .utils import get_data, rename_col_ord


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


@tabled
class TestAliased:
    a: int = field("int32", aliases=["f", "g"])
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


def get_expect():
    data = get_data()
    table = pa.table(
        [
            data.get("a"),
            data.get("b"),
            data.get("c"),
            data.get("d"),
            data.get("e"),
        ],
        schema=pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
                pa.field("d", pa.date64()),
                pa.field("e", pa.date32()),
            ]
        ),
    )
    return table


def deep_equals(ground: pa.Table, test: pa.Table):
    for col in ground.itercolumns():
        tc = test.column(col._name)
        assert col.equals(tc)


def test_base():
    expect = get_expect()
    t = TestUnfielded.from_existing(pa.table(get_data()))
    deep_equals(expect, t)

    t = TestUnfielded.from_columns(get_data())
    deep_equals(expect, t)


def test_aliased():
    expect = get_expect()
    data = get_data()

    test = pa.table(data)
    test.rename_columns({"a": "f", "b": "b", "c": "c", "d": "d", "e": "e"})
    t = TestAliased.from_existing(test)
    deep_equals(expect, t)

    test_dict = rename_col_ord(data, a="f")
    t = TestAliased.from_columns(test_dict)
    deep_equals(expect, t)

    test_dict = rename_col_ord(data, a="g")
    t = TestAliased.from_columns(test_dict)
    deep_equals(expect, t)
