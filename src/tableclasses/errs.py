from typing import Callable

from tableclasses.base.field import FieldMeta

GetRepr = Callable[[FieldMeta], str]


class DataError(Exception):
    def __init__(self, cause: str):
        msg = f"The provided data is invalid: {cause}"
        super().__init__(self, msg)


class ColumnError(Exception):
    def __init__(
        self,
        meta: FieldMeta,
        allowed: GetRepr,
        given: any,
    ):
        msg = f"Expected column {meta.col_name} of type {allowed(meta)}. Given: {type(given)}"
        super().__init__(self, msg)


class UnsupportedTypeError(Exception):
    def __init__(
        self,
        native: type,
        alias: str,
    ):
        head = "The provided"
        tail = "is not supported"
        body = f"{native}"
        if alias != "":
            body += f" ({alias})"
        msg = " ".join((head, body, tail))
        super().__init__(msg)


class RowError(Exception):
    def __init__(self, row: any, allowed: str):
        msg = f"The given row(s) are not valid, and should instead be {allowed}. Given: {row}"
        super().__init__(msg)
