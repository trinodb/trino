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
package io.trino.plugin.duckdb;

import io.airlift.log.Logger;
import io.trino.testing.sql.JdbcSqlExecutor;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TestingDuckDb
        implements Closeable
{
    public static final String TPCH_SCHEMA = "tpch";
    private static final Logger log = Logger.get(TestingDuckDb.class);

    private final Path path;

    public TestingDuckDb()
            throws IOException
    {
        path = Files.createTempFile(null, null);
        Files.delete(path);
    }

    public String getJdbcUrl()
    {
        return "jdbc:duckdb:" + path.toString();
    }

    public void execute(@Language("SQL") String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), new Properties());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute statement '" + sql + "'", e);
        }
    }

    public JdbcSqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(getJdbcUrl(), new Properties());
    }

    @Override
    public void close()
    {
        try {
            Files.delete(path);
        }
        catch (IOException e) {
            log.warn(e, "Deleting file failed: '%s'", path);
        }
    }
}
