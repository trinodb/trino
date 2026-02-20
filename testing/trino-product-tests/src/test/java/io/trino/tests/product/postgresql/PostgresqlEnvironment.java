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
package io.trino.tests.product.postgresql;

import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Abstract base class for PostgreSQL product test environments.
 * <p>
 * This allows tests to be written against the common interface and run
 * in multiple PostgreSQL environment variants (basic, spooling, etc.).
 */
public abstract class PostgresqlEnvironment
        extends ProductTestEnvironment
{
    /**
     * Creates a direct connection to PostgreSQL for test setup/teardown.
     */
    public abstract Connection createPostgresqlConnection()
            throws SQLException;

    /**
     * Executes a SQL query directly against PostgreSQL (not via Trino).
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executePostgresql(String sql)
    {
        try (Connection conn = createPostgresqlConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute PostgreSQL query: " + sql, e);
        }
    }
}
