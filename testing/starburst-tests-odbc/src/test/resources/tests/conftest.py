#
# Copyright Starburst Data, Inc. All rights reserved.
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#
# Redistribution of this material is strictly prohibited.
#

import os
import pytest
from contextlib import closing
from . import Configuration, TestingStarburst
from . import format_connection_string, pyodbc_connect


@pytest.fixture(scope="session")
def configuration():
    return Configuration()


@pytest.fixture(scope="function")
def cursor(connection):
    with closing(connection.cursor()) as cursor:
        yield cursor


@pytest.fixture(scope="module")
def testing_starburst():
    return TestingStarburst(
        os.environ['TRINO_HOST'],
        os.environ['TRINO_PORT'],
    )


@pytest.fixture(scope="function")
def connection(configuration, testing_starburst):
    connection_string = format_connection_string(configuration, testing_starburst)

    with pyodbc_connect(connection_string, timeout=10) as connection:
        yield connection


@pytest.fixture(scope="function")
def connection_with_SQL_ATTR_METADATA_ID_enabled(configuration, testing_starburst):
    yield from _new_connection(
        configuration, testing_starburst, attrs_before={SQL_ATTR_METADATA_ID: SQL_TRUE}
    )


def _new_connection(configuration, testing_starburst, **kwargs):
    connection_string = format_connection_string(configuration, testing_starburst)
    with pyodbc_connect(connection_string, **kwargs) as connection:
        yield connection


@pytest.fixture(scope="function")
def america_ny_tzid_connection(configuration, testing_starburst):
    yield from _new_timezoneid_connection(
        configuration, testing_starburst, "America/New_York"
    )


def _new_timezoneid_connection(configuration, testing_starburst, timezone_id, **kwargs):
    connection_string = format_connection_string(configuration, testing_starburst)
    connection_string += f";TimeZoneID={timezone_id}"

    with pyodbc_connect(connection_string, **kwargs) as connection:
        yield connection
