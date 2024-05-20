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
package io.trino.plugin.iceberg.catalog.snowflake;

import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;

public class TestingSnowflakeServer
{
    public enum TableType
    {
        NATIVE, ICEBERG
    }

    private static final Logger LOG = Logger.get(TestingSnowflakeServer.class);

    public static final String SNOWFLAKE_JDBC_URI = requiredNonEmptySystemProperty("testing.snowflake.catalog.account-url");
    public static final String SNOWFLAKE_USER = requiredNonEmptySystemProperty("testing.snowflake.catalog.user");
    public static final String SNOWFLAKE_PASSWORD = requiredNonEmptySystemProperty("testing.snowflake.catalog.password");
    public static final String SNOWFLAKE_ROLE = requiredNonEmptySystemProperty("testing.snowflake.catalog.role");
    public static final String SNOWFLAKE_WAREHOUSE = requiredNonEmptySystemProperty("testing.snowflake.catalog.warehouse");
    public static final String SNOWFLAKE_TEST_DATABASE = requiredNonEmptySystemProperty("testing.snowflake.catalog.database");

    public void execute(String schema, String sql)
            throws SQLException
    {
        checkArgument(sql != null && !sql.isEmpty(), "sql can not be null or empty");
        executeOnDatabaseWithResultSetOperator(schema, Optional.empty(), sql);
    }

    public boolean checkIfTableExists(TableType tableType, String schema, String tableName)
            throws SQLException
    {
        Function<ResultSet, Boolean> tableExistsOperator = resultSet -> {
            try {
                return resultSet.next();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        String sql = "SHOW %sTABLES LIKE '%s'".formatted(tableType == TableType.ICEBERG ? "ICEBERG " : "", tableName);

        return executeOnDatabaseWithResultSetOperator(schema, Optional.of(tableExistsOperator), sql).orElseThrow();
    }

    private <T> Optional<T> executeOnDatabaseWithResultSetOperator(String schema, Optional<Function<ResultSet, T>> function, String sql)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.put("user", SNOWFLAKE_USER);
        properties.put("password", SNOWFLAKE_PASSWORD);
        properties.put("role", SNOWFLAKE_ROLE);
        properties.put("warehouse", SNOWFLAKE_WAREHOUSE);
        properties.put("db", SNOWFLAKE_TEST_DATABASE);
        properties.put("schema", schema);

        try (Connection connection = DriverManager.getConnection(SNOWFLAKE_JDBC_URI, properties);
                Statement statement = connection.createStatement()) {
            LOG.info("Using user: %s, role: %s, warehouse: %s, database: %s, schema: %s to execute: [%s]", SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_TEST_DATABASE, schema, sql);
            statement.execute(sql);

            return function.map(func -> {
                try {
                    return func.apply(statement.getResultSet());
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
