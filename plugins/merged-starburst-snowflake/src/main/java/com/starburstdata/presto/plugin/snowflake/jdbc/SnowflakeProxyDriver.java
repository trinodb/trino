/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.snowflake.client.jdbc.SnowflakeDriver;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

public class SnowflakeProxyDriver
        implements java.sql.Driver
{
    private final SnowflakeDriver driver = new SnowflakeDriver();

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        Connection connection = driver.connect(url, info);
        try (Statement statement = connection.createStatement()) {
            // this is required so that long Snowflake NUMBERs are not casted to Java Long which overflows
            statement.execute("ALTER SESSION SET JDBC_TREAT_DECIMAL_AS_INT=false");
            statement.execute("ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'");
            statement.execute("ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'");
            statement.execute("ALTER SESSION SET TIMESTAMP_TZ_OUTPUT_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'");
            statement.execute("ALTER SESSION SET TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'");
            statement.execute("ALTER SESSION SET TIME_OUTPUT_FORMAT='HH24:MI:SS.FF9'");
            statement.execute("ALTER SESSION SET JSON_INDENT=0");
        }

        return connection;
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return driver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        return driver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion()
    {
        return driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion()
    {
        return driver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant()
    {
        return driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        return driver.getParentLogger();
    }
}
