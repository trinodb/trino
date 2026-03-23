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
package io.trino.plugin.databricks;

import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;

public final class TestingDatabricksServer
{
    private TestingDatabricksServer() {}

    public static final String TEST_URL = requiredNonEmptySystemProperty("databricks.test.server.url");
    public static final String TEST_TOKEN = requiredNonEmptySystemProperty("databricks.test.server.token");
    public static final String TEST_SCHEMA = "default";

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
        properties.setProperty("PWD", TEST_TOKEN);
        properties.setProperty("UID", "token");
        return properties;
    }
}
