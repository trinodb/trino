#
# Copyright Starburst Data, Inc. All rights reserved.
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#
# Redistribution of this material is strictly prohibited.
#

import pyodbc
from math import inf, nan, isnan
from decimal import Decimal


def test_column_name(cursor):
    for column_alias in ["colaa", "xxxax"]:
        for expression in ["1", "'aaa'", "current_time"]:
            cursor.execute("select {0} as {1}".format(expression, column_alias))
            assert _column_description(cursor)[0] == column_alias


_expressions_and_types = [
    # (expression, type code, expected python value)
    ("1", pyodbc.SQL_INTEGER, 1),
    ("12345", pyodbc.SQL_INTEGER, 12345),
    ("123456789", pyodbc.SQL_INTEGER, 123456789),
    ("9223372036854775807", pyodbc.SQL_BIGINT, 9223372036854775807),
    ("REAL '12345.25'", pyodbc.SQL_REAL, 12345.25),
    (
        "DOUBLE '12345.00390625'",
        pyodbc.SQL_DOUBLE,
        12345.00390625,
    ),  # 0.00390625 == 1/256
    ("1 / 0E0", pyodbc.SQL_DOUBLE, inf),
    ("-1 / 0E0", pyodbc.SQL_DOUBLE, -inf),
    ("0 / 0E0", pyodbc.SQL_DOUBLE, nan),
    ("DECIMAL '12345.54321'", pyodbc.SQL_DECIMAL, Decimal("12345.54321")),
    ("'1'", pyodbc.SQL_WVARCHAR, "1"),
    ("'abc'", pyodbc.SQL_WVARCHAR, "abc"),
    ("'Łąka jest piękna.'", pyodbc.SQL_WVARCHAR, "Łąka jest piękna."),
    ("CAST('abc' AS char(5))", pyodbc.SQL_WCHAR, "abc  "),
    ("X'1234567890abcdef'", pyodbc.SQL_VARBINARY, b"\x12\x34\x56\x78\x90\xab\xcd\xef"),
    ("true", pyodbc.SQL_BIT, True),
    ("false", pyodbc.SQL_BIT, False),
    ("ARRAY[1, 2]", pyodbc.SQL_WVARCHAR, "[1,2]"),
]


def test_type_codes(connection, cursor):
    constant_names = {}
    for n in dir(pyodbc):
        v = getattr(pyodbc, n)
        if n.startswith("SQL_") and type(v) == int:
            constant_names.setdefault(v, []).append(n)

    def _mock_converter(type_code):
        type_explanation = "Type {type_code} (this might be: {names})".format(
            type_code=type_code, names=constant_names[type_code]
        )
        return lambda buffer: type_explanation

    for type_code in range(-10000, 20000):
        if type_code in constant_names:
            connection.add_output_converter(type_code, _mock_converter(type_code))

    for expression, type_code, _ in _expressions_and_types:
        # Hacky way to know type code of selected expression.
        # See https://github.com/mkleehammer/pyodbc/issues/167
        this_is_it = "Type of {expression} should be {type_code}".format(
            expression=expression, type_code=type_code
        )
        connection.add_output_converter(type_code, lambda buffer: this_is_it)
        assert cursor.execute("select " + expression).fetchval() == this_is_it, (
            "Expression " + expression
        )
        connection.add_output_converter(type_code, _mock_converter(type_code))


def test_expression_values(cursor):
    for expression, _, expected in _expressions_and_types:
        actual = cursor.execute("select " + expression).fetchval()
        message = (
            "Expression {expression} produced {actual}, expected {expected}".format(
                expression=expression, actual=actual, expected=expected
            )
        )
        if _isnan(expected):
            assert isnan(actual), message
        else:
            assert actual == expected, message


def _column_description(cursor):
    assert len(cursor.description) == 1
    return cursor.description[0]


def _isnan(any):
    if any != any:
        # This might be nan, make sure
        return isnan(any)

    # This can be anything, but not nan
    return False
