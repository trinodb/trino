#
# Copyright Starburst Data, Inc. All rights reserved.
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#
# Redistribution of this material is strictly prohibited.
#

from contextlib import closing

import datetime as dt


def test_timestamptz_with_precision(cursor):
    cursor.execute(
        "SELECT * FROM (VALUES ("
        "CAST(TIMESTAMP '2020-05-01 12:34:56.12344' AS TIMESTAMP(4) WITH TIME ZONE)))  "
        "AS t "
    )
    assert cursor.fetchval() == "2020-05-01 12:34:56.1234 UTC"
    cursor.execute(
        "SELECT * FROM (VALUES ("
        "CAST(TIMESTAMP '2020-05-01 12:34:56.12348' AS TIMESTAMP(4) WITH TIME ZONE)))  "
        "AS t "
    )
    assert cursor.fetchval() == "2020-05-01 12:34:56.1235 UTC"


def test_timestamptz(cursor):
    cursor.execute(
        "SELECT * FROM (VALUES ("
        "CAST(TIMESTAMP '2020-05-01 12:34:56.12344' AS TIMESTAMP WITH TIME ZONE)))  "
        "AS t "
    )
    assert cursor.fetchval() == "2020-05-01 12:34:56.123 UTC"


def test_timestamp(cursor):
    cursor.execute(
        "SELECT * FROM (VALUES (" "TIMESTAMP '2012-03-14 1:59:26.535'))" "AS t "
    )
    cursor.fetchval() == dt.datetime(2012, 3, 14, 1, 59, 26, 535)


def test_timestamptz_with_non_utc(america_ny_tzid_connection):
    with closing(america_ny_tzid_connection.cursor()) as cursor:
        cursor.execute(
            "SELECT * FROM (VALUES ("
            "CAST(TIMESTAMP '2020-05-01 12:34:56.12348' "
            "AS TIMESTAMP(4) WITH TIME ZONE))) "
            "AS t "
        )
        assert cursor.fetchval() == "2020-05-01 12:34:56.1235 America/New_York"
