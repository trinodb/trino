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

import io.airlift.log.Logger;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.ProductTestEnvironment.TrinoSession;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

/**
 * JUnit 5 port of TestHivePartitionSchemaEvolution.
 * <p>
 * Tests partition schema evolution in Hive tables with ORC and Parquet formats.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHivePartitionSchemaEvolution
{
    private static final Logger log = Logger.get(TestHivePartitionSchemaEvolution.class);

    static Stream<String> storageFormats()
    {
        return Stream.of("PARQUET", "ORC");
    }

    @ParameterizedTest
    @MethodSource("storageFormats")
    void testPartitionSchemaEvolution(String format, HiveBasicEnvironment env)
    {
        String tableName = "schema_evolution_" + randomNameSuffix();
        try {
            // Create the test table
            createTable(env, tableName, format);

            // Execute all evolution tests within a single session where session properties are set
            env.executeTrinoInSession(session -> {
                // Set session properties to use column mapping by name
                session.executeUpdate("SET SESSION hive.parquet_use_column_names = true");
                session.executeUpdate("SET SESSION hive.orc_use_column_names = true");

                // dropping column on table, simulates creating a column on partition
                // adding column on table, simulates dropping a column on partition

                // partition is adding a column at the start
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (float_column float, varchar_column varchar(20))", row(1.1, "jeden", 1));

                // partition is adding a column in the middle
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (int_column int, varchar_column varchar(20))", row(1, "jeden", 1));

                // partition is adding a column at the end
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (int_column int, float_column float)", row(1, 1.1, 1));

                // partition is dropping a column at the start
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (tiny_column tinyint, int_column int, float_column float, varchar_column varchar(20))", row(null, 1, 1.1, "jeden", 1));

                // partition is dropping a column in the middle
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (int_column int, tiny_column tinyint, float_column float, varchar_column varchar(20))", row(1, null, 1.1, "jeden", 1));

                // partition is dropping a column at the end
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (int_column int, float_column float, varchar_column varchar(20), tiny_column tinyint)", row(1, 1.1, "jeden", null, 1));

                // partition is dropping and adding column in the middle
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (int_column int, tiny_column tinyint, varchar_column varchar(20))", row(1, null, "jeden", 1));

                // partition is adding coercions
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (int_column bigint, float_column double, varchar_column varchar(20))", row(1, 1.1, "jeden", 1));

                // partition is swapping columns with coercions
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (varchar_column varchar(20), float_column double, int_column bigint)", row("jeden", 1.1, 1, 1));

                // partition is swapping columns and partition with coercions and is adding a column
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (float_column double, int_column bigint)", row(1.1, 1, 1));

                // partition is swapping columns and partition with coercions and is removing a column
                testEvolution(env, session, tableName, "ALTER TABLE %s REPLACE COLUMNS (varchar_column varchar(20), tiny_column tinyint, float_column double, int_column bigint)", row("jeden", null, 1.100000023841858, 1L, 1L));
            });
        }
        finally {
            // Clean up the test table
            try {
                env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
            }
            catch (Exception e) {
                log.warn(e, "Failed to drop table %s", tableName);
            }
        }
    }

    private void testEvolution(HiveBasicEnvironment env, TrinoSession session, String tableName, String sql, Row expectedRow)
            throws SQLException
    {
        if (tryExecuteOnHive(env, format(sql, tableName))) {
            assertThat(session.executeQuery("SELECT * FROM " + tableName))
                    .contains(expectedRow);
        }
    }

    private boolean tryExecuteOnHive(HiveBasicEnvironment env, String sql)
    {
        try {
            env.executeHiveUpdate(sql);
            return true;
        }
        catch (RuntimeException e) {
            String message = fullErrorMessage(e);
            if (message != null &&
                    (message.contains("Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions")
                            || message.contains("Replacing columns cannot drop columns")
                            || message.contains("Replace columns is not supported for"))) {
                log.warn("Unable to execute: %s, due: %s", sql, message);
                return false;
            }
            throw e;
        }
    }

    private static String fullErrorMessage(Throwable throwable)
    {
        StringBuilder builder = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                if (!builder.isEmpty()) {
                    builder.append(" | ");
                }
                builder.append(current.getMessage());
            }
            current = current.getCause();
        }
        return builder.toString();
    }

    private void createTable(HiveBasicEnvironment env, String tableName, String format)
    {
        tryExecuteOnHive(env, format(
                "CREATE TABLE %s (" +
                        "  int_column int," +
                        "  float_column float," +
                        "  varchar_column varchar(20)" +
                        ") " +
                        "PARTITIONED BY (partition_column bigint) " +
                        "STORED AS %s",
                tableName,
                format));

        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (1, 1.1, 'jeden', 1)", tableName));
    }
}
