#
# Copyright Starburst Data, Inc. All rights reserved.
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#
# Redistribution of this material is strictly prohibited.
#

import pytest
import pyodbc
from contextlib import closing
from . import TestingStarburst
from . import format_connection_string, pyodbc_connect


def test_initiate_tests(testing_starburst):
    assert testing_starburst is not None


def test_connect(configuration, testing_starburst):
    connection_string = format_connection_string(configuration, testing_starburst)
    connection = pyodbc.connect(connection_string, timeout=10)
    connection.close()


def test_smoke_test(cursor):
    assert cursor.execute("select count(*) from tpch.tiny.nation").fetchval() == 25

@pytest.mark.parametrize(
    "flags",
    [
        {},
        {"autocommit": True},
        {"autocommit": False},
    ],
)
def test_connect_with_flags(configuration, testing_starburst, flags):
    connection_string = format_connection_string(configuration, testing_starburst)
    connection = pyodbc.connect(connection_string, timeout=10, **flags)
    connection.close()

    connection = pyodbc.connect(connection_string, timeout=10, **flags)
    try:
        cursor = connection.cursor()
        try:
            assert cursor.execute("select 42").fetchval() == 42
        finally:
            cursor.close()
    finally:
        connection.close()


def test_connect_no_schema(configuration, testing_starburst):
    connection_string = format_connection_string(configuration, testing_starburst)

    with pyodbc_connect(connection_string, timeout=10) as connection:
        with pytest.raises(pyodbc.Error) as exc_info:
            connection.cursor().execute("select count(*) from nation")
        assert "Schema must be specified when session schema is not set" in str(
            exc_info.value
        )
        assert (
            connection.cursor()
            .execute("select count(*) from tpch.tiny.nation")
            .fetchval()
            == 25
        )

def test_connect_to_schema(configuration, testing_starburst):
    connection_string = format_connection_string(configuration, testing_starburst)
    connection_string += ";catalog=tpch;schema=tiny"

    with pyodbc_connect(connection_string, timeout=10) as connection, closing(
        connection.cursor()
    ) as cursor:
        assert cursor.execute("select count(*) from nation").fetchval() == 25
