from datetime import date, datetime

import pyarrow as pa

from tableclasses.arrow import tabled
from tableclasses.base.field import field

from .utils import get_data, get_row_dicts, not_caught


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
