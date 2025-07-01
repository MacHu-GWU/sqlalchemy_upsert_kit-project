# -*- coding: utf-8 -*-

import typing as T

from prettytable import PrettyTable


def from_many_dict(
    data: T.Iterable[dict[str, T.Any]],
) -> T.Optional[PrettyTable]:
    """
    Convert an iterable of dictionaries to a PrettyTable.

    :param data: An iterable of dictionaries, where each dictionary represents a row.

    :return: A PrettyTable object containing the data, or None if no data is provided.
    """

    # try:
    iterator = iter(data)
    try:
        first_row = next(iterator)
        if first_row is None:
            return None
    except StopIteration:
        return None

    tb = PrettyTable()
    tb.field_names = list(first_row.keys())
    tb.add_row(list(first_row.values()))
    for row in iterator:
        tb.add_row(list(row.values()))
    return tb


if __name__ == "__main__":

    def test_from_many_dict():
        def iter_rows():
            data = [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
            for row in data:
                yield row

        pt = from_many_dict(iter_rows())
        print(pt)
        assert pt.field_names == ["id", "name"]
        assert pt.rows == [
            [1, "Alice"],
            [2, "Bob"],
        ]

    test_from_many_dict()

    def test_from_many_dict_no_data():
        def iter_rows():
            return []

        pt = from_many_dict(iter_rows())
        print(pt)
        assert pt is None

    test_from_many_dict_no_data()
