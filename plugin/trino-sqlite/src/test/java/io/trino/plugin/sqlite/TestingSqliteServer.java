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
package io.trino.plugin.sqlite;

import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import org.sqlite.SQLiteOpenMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TestingSqliteServer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(TestingSqliteServer.class);

    private static final String DEFAULT_USER = "SA";
    private static final String DEFAULT_PASSWORD = "";
    private static final String SQLITE_URL = "jdbc:sqlite:";
    private static final String SQLITE_DB = "sqlite.db";
    private static final String SQLITE_JOURNAL_MODE = "WAL";
    private static final int SQLITE_OPEN_MODE = SQLiteOpenMode.READWRITE.flag |
            SQLiteOpenMode.CREATE.flag |
            SQLiteOpenMode.NOMUTEX.flag;
    private static final String SQLITE_PATH;

    static {
        try {
            SQLITE_PATH = Path.of(Files.createTempDirectory("sqlite").toFile().getAbsolutePath()).toUri().toString();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isRunning;

    public TestingSqliteServer()
    {
        isRunning = true;
        log.info("%s listening on url: %s", TestingSqliteServer.class.getName(), getJdbcUrl());
    }

    public SqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(getJdbcUrl(), getProperties());
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = getConnection(user, password);
                Statement statement = connection.createStatement()) {
            log.info("TestingSqliteServer.execute() SQL query: " + sql);
            statement.execute(sql);
        }
        catch (SQLException e) {
            log.error(e, "SQL query: " + sql);
            throw new RuntimeException(e);
        }
    }

    public String getUsername()
    {
        return DEFAULT_USER;
    }

    public String getPassword()
    {
        return DEFAULT_PASSWORD;
    }

    public String getJdbcUrl()
    {
        return SQLITE_URL + SQLITE_PATH + SQLITE_DB;
    }

    public Connection getConnection()
            throws SQLException
    {
        return getConnection(getUsername(), getPassword());
    }

    private Connection getConnection(String user, String password)
            throws SQLException
    {
        return DriverManager.getConnection(getJdbcUrl(), getProperties(user, password));
    }

    private Properties getProperties()
    {
        return getProperties(getUsername(), getPassword());
    }

    private Properties getProperties(String user, String password)
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("journal_mode", SQLITE_JOURNAL_MODE);
        properties.setProperty("open_mode", Integer.toString(SQLITE_OPEN_MODE));
        return properties;
    }

    @Override
    public void close()
    {
        isRunning = false;
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return isRunning;
    }
}
