#
# Copyright Starburst Data, Inc. All rights reserved.
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#
# Redistribution of this material is strictly prohibited.
#

import datetime as dt
import random
import string
from contextlib import closing

# passing parameters to pyodbc.cursor.execute() is causing usage of prepared statements
# in odbc, see:
# https://github.com/mkleehammer/pyodbc/wiki/Getting-started#parameters


def test_prepared_statements(cursor):
    cursor.execute("SELECT * FROM tpch.tiny.orders WHERE orderkey = ?", 1)
    _consume_output(cursor)


def test_prepared_statements_with_many_params(cursor):
    predicates_count = 100
    sql = "SELECT * FROM tpch.tiny.orders WHERE ({predicates})".format(
        predicates=" AND ".join(["orderkey = ?" for i in range(predicates_count)])
    )
    cursor.execute(sql, *[i for i in range(predicates_count)])
    _consume_output(cursor)


def test_long_prepared_statements(cursor):
    predicates_count = 14
    sql = "SELECT * FROM tpch.tiny.orders WHERE ({predicates})".format(
        predicates=" AND ".join(["comment = ?" for i in range(predicates_count)])
    )
    cursor.execute(
        sql,
        *[
            "".join(random.choices(string.ascii_uppercase + string.digits, k=1000))
            for i in range(predicates_count)
        ]
    )
    _consume_output(cursor)


def test_execute_with_parameterized_timestamps_input(cursor):
    cursor.execute(
        "SELECT 1 FROM (VALUES ("
        "TIMESTAMP '2012-03-14 1:59:26.535',"
        "CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP WITH TIME ZONE))) "
        "AS t (t_timestamp, t_timestamptz) "
        "WHERE t_timestamp = ? AND t_timestamptz = ?",
        dt.datetime(2012, 3, 14, 1, 59, 26, 535),
        dt.datetime(
            2020, 5, 1, 12, 34, 56, 123, tzinfo=dt.timezone(dt.timedelta(seconds=19800))
        ),
    )
    _consume_output(cursor)


def test_execute_with_parameterized_timestamps_output(cursor):
    cursor.execute(
        "SELECT t_timestamp, t_timestamptz FROM (VALUES ("
        "TIMESTAMP '2012-03-14 1:59:26.535',"
        "CAST(TIMESTAMP '2020-05-01 12:34:56.123' AS TIMESTAMP WITH TIME ZONE))) "
        "AS t (t_timestamp, t_timestamptz) "
        "WHERE t_timestamp = ? AND t_timestamptz = ?",
        dt.datetime(2012, 3, 14, 1, 59, 26, 535),
        dt.datetime(
            2020, 5, 1, 12, 34, 56, 123, tzinfo=dt.timezone(dt.timedelta(seconds=19800))
        ),
    )
    _consume_output(cursor)


def test_execute_with_parameterized_timestamps_precision_input(cursor):
    cursor.execute(
        "SELECT 1 FROM (VALUES ("
        "TIMESTAMP '2012-03-14 1:59:26.535897',"
        "CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(5) WITH TIME ZONE)))  "
        "AS t (t_with_precision_timestamp, t_with_precision_timestamptz) "
        "WHERE t_with_precision_timestamp = ? AND t_with_precision_timestamptz = ?",
        dt.datetime(2012, 3, 14, 1, 59, 26, 535897),
        dt.datetime(
            2020,
            5,
            1,
            12,
            34,
            56,
            12345,
            tzinfo=dt.timezone(dt.timedelta(seconds=19800)),
        ),
    )
    _consume_output(cursor)


def test_execute_with_parameterized_timestamps_precision_output(cursor):
    cursor.execute(
        "SELECT t_with_precision_timestamp, t_with_precision_timestamptz FROM (VALUES ("
        "TIMESTAMP '2012-03-14 1:59:26.535897',"
        "CAST(TIMESTAMP '2020-05-01 12:34:56.12345' AS TIMESTAMP(5) WITH TIME ZONE)))  "
        "AS t (t_with_precision_timestamp, t_with_precision_timestamptz) "
        "WHERE t_with_precision_timestamp = ? AND t_with_precision_timestamptz = ?",
        dt.datetime(2012, 3, 14, 1, 59, 26, 535897),
        dt.datetime(
            2020,
            5,
            1,
            12,
            34,
            56,
            12345,
            tzinfo=dt.timezone(dt.timedelta(seconds=19800)),
        ),
    )
    _consume_output(cursor)


def _consume_output(cursor):
    while True:
        chunk = cursor.fetchmany(10000)
        if not chunk:
            break
