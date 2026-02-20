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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.Row;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests interactions between Iceberg and Hive connectors when one reads a view created by the other.
 */
abstract class BaseTestIcebergHiveViewsCompatibility
{
    @Test
    void testIcebergHiveViewsCompatibility(ProductTestEnvironment env)
    {
        try {
            cleanup(env);

            List<Row> hivePreexistingTables = env.executeTrino("SHOW TABLES FROM hive.default").getRows();
            List<Row> icebergPreexistingTables = env.executeTrino("SHOW TABLES FROM iceberg.default").getRows();

            env.executeTrinoUpdate("CREATE TABLE hive.default.hive_table AS SELECT 1 bee");
            env.executeTrinoUpdate("CREATE TABLE iceberg.default.iceberg_table AS SELECT 2 snow");

            env.executeTrinoInSession(session -> {
                session.executeUpdate("USE hive.default");
                session.executeUpdate("CREATE VIEW hive.default.hive_view_qualified_hive AS SELECT * FROM hive.default.hive_table");
                session.executeUpdate("CREATE VIEW hive.default.hive_view_unqualified_hive AS SELECT * FROM hive_table");
                session.executeUpdate("CREATE VIEW hive.default.hive_view_qualified_iceberg AS SELECT * FROM iceberg.default.iceberg_table");
                assertThatThrownBy(() -> session.executeUpdate("CREATE VIEW hive.default.hive_view_unqualified_iceberg AS SELECT * FROM iceberg_table"))
                        .hasMessageContaining("Cannot query Iceberg table 'default.iceberg_table'");
            });

            env.executeTrinoInSession(session -> {
                session.executeUpdate("USE iceberg.default");
                session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_qualified_hive AS SELECT * FROM hive.default.hive_table");
                assertThatThrownBy(() -> session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_unqualified_hive AS SELECT * FROM hive_table"))
                        .hasMessageContaining("Not an Iceberg table: default.hive_table");
                session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_qualified_iceberg AS SELECT * FROM iceberg.default.iceberg_table");
                session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_unqualified_iceberg AS SELECT * FROM iceberg_table");
            });

            List<Row> newlyCreated = List.of(
                    row("hive_table"),
                    row("iceberg_table"),
                    row("hive_view_qualified_hive"),
                    row("hive_view_unqualified_hive"),
                    row("hive_view_qualified_iceberg"),
                    row("iceberg_view_qualified_hive"),
                    row("iceberg_view_qualified_iceberg"),
                    row("iceberg_view_unqualified_iceberg"));

            List<Row> expectedHiveTables = new ArrayList<>(hivePreexistingTables);
            expectedHiveTables.addAll(newlyCreated);
            assertThat(env.executeTrino("SHOW TABLES FROM hive.default"))
                    .containsOnly(expectedHiveTables);

            List<Row> expectedIcebergTables = new ArrayList<>(icebergPreexistingTables);
            expectedIcebergTables.addAll(newlyCreated);
            assertThat(env.executeTrino("SHOW TABLES FROM iceberg.default"))
                    .containsOnly(expectedIcebergTables);

            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_view_unqualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM hive.default.iceberg_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM hive.default.iceberg_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM hive.default.iceberg_view_unqualified_iceberg")).containsOnly(row(2));

            assertThat(env.executeTrino("SELECT * FROM iceberg.default.hive_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.hive_view_unqualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.hive_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.iceberg_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.iceberg_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.iceberg_view_unqualified_iceberg")).containsOnly(row(2));
        }
        finally {
            cleanup(env);
        }
    }

    private static void cleanup(ProductTestEnvironment env)
    {
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_qualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_unqualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_qualified_iceberg");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_unqualified_iceberg");

        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_qualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_unqualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_qualified_iceberg");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_unqualified_iceberg");

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.hive_table");
        env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.iceberg_table");
    }
}
