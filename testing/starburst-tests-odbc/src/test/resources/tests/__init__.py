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
from contextlib import contextmanager, closing

SQL_FALSE = 0
SQL_TRUE = 1

SQL_ATTR_METADATA_ID = 10014


class TestingStarburst(object):
    __test__ = False

    def __init__(self, host, port):
        self.host = host
        self.port = port


class Configuration(object):
    def driver_name(self):
        return "Starburst ODBC Driver 64-bit"


@contextmanager
def pyodbc_connect(connection_string, **kwargs):
    kwargs.setdefault("autocommit", True)
    kwargs.setdefault("timeout", 10)
    kwargs.setdefault("attrs_before", {SQL_ATTR_METADATA_ID: SQL_FALSE})
    with closing(pyodbc.connect(connection_string, **kwargs)) as connection:
        yield connection


def format_connection_string(configuration, testing_starburst):
    connection_string = (
        "driver={driver};host={host};port={port};ssl=0"
    ).format(
        driver=configuration.driver_name(),
        host=testing_starburst.host,
        port=testing_starburst.port,
    )
    return connection_string
