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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.hive.HiveIcebergRedirectionsEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * Tests for Iceberg metadata listing including Hive tables and views.
 * <p>
 * Ported from the Tempto-based TestIcebergHiveMetadataListing.
 */
@ProductTest
@RequiresEnvironment(HiveIcebergRedirectionsEnvironment.class)
@TestGroup.HiveIcebergRedirections
class TestIcebergHiveMetadataListing
{
    @Test
    void testTableListing(HiveIcebergRedirectionsEnvironment env)
    {
        try {
            cleanup(env);

            List<Row> preexistingTables = env.executeTrino("SHOW TABLES FROM iceberg.default").getRows();

            env.executeTrinoUpdate("CREATE TABLE iceberg.default.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
            env.executeTrinoUpdate("CREATE MATERIALIZED VIEW iceberg.default.iceberg_materialized_view AS " +
                    "SELECT * FROM iceberg.default.iceberg_table1");

            env.executeTrinoUpdate("CREATE TABLE hive.default.hive_table (_double DOUBLE)");
            env.executeTrinoUpdate("CREATE VIEW hive.default.hive_view AS SELECT * FROM hive.default.hive_table");
            env.executeTrinoUpdate("CREATE VIEW iceberg.default.iceberg_view AS SELECT * FROM iceberg.default.iceberg_table1");

            List<Row> expectedTables = new ArrayList<>(preexistingTables);
            expectedTables.add(row("iceberg_table1"));
            expectedTables.add(row("iceberg_materialized_view"));
            expectedTables.add(row("iceberg_view"));
            expectedTables.add(row("hive_table"));
            // Iceberg connector supports Trino views created via Hive connector
            expectedTables.add(row("hive_view"));

            assertThat(env.executeTrino("SHOW TABLES FROM iceberg.default"))
                    .containsOnly(expectedTables);
        }
        finally {
            cleanup(env);
        }
    }

    @Test
    void testColumnListing(HiveIcebergRedirectionsEnvironment env)
    {
        try {
            cleanup(env);

            List<Row> preexistingColumns = env.executeTrino(
                    "SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                            "WHERE table_catalog = 'iceberg' AND table_schema = 'default'").getRows();

            env.executeTrinoUpdate("CREATE TABLE iceberg.default.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
            env.executeTrinoUpdate("CREATE MATERIALIZED VIEW iceberg.default.iceberg_materialized_view AS " +
                    "SELECT * FROM iceberg.default.iceberg_table1");

            env.executeTrinoUpdate("CREATE TABLE hive.default.hive_table (_double DOUBLE)");
            env.executeTrinoUpdate("CREATE VIEW hive.default.hive_view AS SELECT * FROM hive.default.hive_table");
            env.executeTrinoUpdate("CREATE VIEW iceberg.default.iceberg_view AS SELECT * FROM iceberg.default.iceberg_table1");

            List<Row> expectedColumns = new ArrayList<>(preexistingColumns);
            expectedColumns.add(row("iceberg_table1", "_string"));
            expectedColumns.add(row("iceberg_table1", "_integer"));
            expectedColumns.add(row("iceberg_materialized_view", "_string"));
            expectedColumns.add(row("iceberg_materialized_view", "_integer"));
            expectedColumns.add(row("iceberg_view", "_string"));
            expectedColumns.add(row("iceberg_view", "_integer"));
            // With bi-directional redirections, Iceberg connector also sees Hive tables
            expectedColumns.add(row("hive_table", "_double"));
            expectedColumns.add(row("hive_view", "_double"));

            assertThat(env.executeTrino(
                    "SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                            "WHERE table_catalog = 'iceberg' AND table_schema = 'default'"))
                    .containsOnly(expectedColumns);
        }
        finally {
            cleanup(env);
        }
    }

    private void cleanup(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view");
        env.executeTrinoUpdate("DROP MATERIALIZED VIEW IF EXISTS iceberg.default.iceberg_materialized_view");
        env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.iceberg_table1");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view");
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.hive_table");
    }
}
