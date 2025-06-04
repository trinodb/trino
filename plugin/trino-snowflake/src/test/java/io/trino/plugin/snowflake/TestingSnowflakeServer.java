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
package io.trino.plugin.snowflake;

import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.snowflake.SnowflakeClientModule.setOutputProperties;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;

public final class TestingSnowflakeServer
{
    private TestingSnowflakeServer() {}

    public static final String TEST_URL = requiredNonEmptySystemProperty("snowflake.test.server.url");
    public static final String TEST_USER = requiredNonEmptySystemProperty("snowflake.test.server.user");
    public static final String TEST_PASSWORD = requiredNonEmptySystemProperty("snowflake.test.server.password");
    public static final String TEST_DATABASE = requiredNonEmptySystemProperty("snowflake.test.server.database");
    public static final String TEST_WAREHOUSE = requiredNonEmptySystemProperty("snowflake.test.server.warehouse");
    public static final Optional<String> TEST_ROLE = Optional.ofNullable(System.getProperty("snowflake.test.server.role"));
    public static final String TEST_SCHEMA = "tpch";

    public static void execute(@Language("SQL") String sql)
    {
        execute(TEST_URL, getProperties(), sql);
    }

    private static void execute(String url, Properties properties, String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties getProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("user", TEST_USER);
        properties.setProperty("password", TEST_PASSWORD);
        properties.setProperty("db", TEST_DATABASE);
        properties.setProperty("schema", TEST_SCHEMA);
        properties.setProperty("warehouse", TEST_WAREHOUSE);
        TEST_ROLE.ifPresent(role -> properties.setProperty("role", role));
        setOutputProperties(properties);
        return properties;
    }
}
