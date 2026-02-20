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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Hive CREATE TABLE operations in the Trino Hive connector.
 * <p>
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
class TestHiveCreateTable
{
    private static final String TRANSACTIONAL = "transactional";

    @Test
    @TestGroup.StorageFormats
    void testCreateTable(HiveStorageFormatsEnvironment env)
            throws SQLException
    {
        env.executeTrinoUpdate("CREATE TABLE test_create_table(a bigint, b varchar, c smallint) WITH (format='ORC')");
        try {
            env.executeTrinoUpdate("INSERT INTO test_create_table(a, b, c) VALUES " +
                    "(NULL, NULL, NULL), " +
                    "(-42, 'abc', SMALLINT '-127'), " +
                    "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz', SMALLINT '32767')");
            assertThat(env.executeTrino("SELECT * FROM test_create_table"))
                    .containsOnly(
                            row(null, null, null),
                            row(-42L, "abc", (short) -127),
                            row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz", (short) 32767));
            Optional<String> transactionalProperty = getTableProperty(env, "test_create_table", TRANSACTIONAL);
            // Hive 3 removes "transactional" table property when it has value "false"
            assertThat(transactionalProperty)
                    .isIn(Optional.empty(), Optional.of("false"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE test_create_table");
        }
    }

    @Test
    @TestGroup.StorageFormats
    void testCreateTableAsSelect(HiveStorageFormatsEnvironment env)
            throws SQLException
    {
        env.executeTrinoUpdate("" +
                "CREATE TABLE test_create_table_as_select WITH (format='ORC') AS " +
                "SELECT * FROM (VALUES " +
                "  (NULL, NULL, NULL), " +
                "  (-42, 'abc', SMALLINT '-127'), " +
                "  (9223372036854775807, 'abcdefghijklmnopqrstuvwxyz', SMALLINT '32767')" +
                ") t(a, b, c)");
        try {
            assertThat(env.executeTrino("SELECT * FROM test_create_table_as_select"))
                    .containsOnly(
                            row(null, null, null),
                            row(-42, "abc", (short) -127),
                            row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz", (short) 32767));
            Optional<String> transactionalProperty = getTableProperty(env, "test_create_table_as_select", TRANSACTIONAL);
            // Hive 3 removes "transactional" table property when it has value "false"
            assertThat(transactionalProperty)
                    .isIn(Optional.empty(), Optional.of("false"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE test_create_table_as_select");
        }
    }

    private static Optional<String> getTableProperty(HiveStorageFormatsEnvironment env, String tableName, String propertyName)
            throws SQLException
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(propertyName, "propertyName is null");

        try (Connection connection = env.createHiveConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW TBLPROPERTIES " + tableName)) {
            while (resultSet.next()) {
                if (propertyName.equals(resultSet.getString("prpt_name"))) {
                    // We need to distinguish between a property that is not set and a property that has NULL value.
                    return Optional.of(resultSet.getString("prpt_value"));
                }
            }
        }

        return Optional.empty();
    }
}
