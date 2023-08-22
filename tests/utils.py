from copy import deepcopy
from datetime import datetime


def not_caught():
    msg = "not caught"
    raise RuntimeError(msg)


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


def get_row_dicts():
    row_dicts = [{} for v in DATA.get("a")]
    for key, val in DATA.items():
        for i, v in enumerate(val):
            if len(row_dicts) < (i - 1):
                row_dicts.append({})
            row_dicts[i][key] = v
    return row_dicts


def rename_col_ord(dl: dict, **keys):
    return {keys.get(k, k): v for k, v in dl.items()}
