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
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.hive.HiveIcebergRedirectionsEnvironment;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests interactions between Iceberg and Hive connectors, when one tries to read a view created by the other.
 * <p>
 * Ported from the Tempto-based TestIcebergHiveViewsCompatibility.
 */
@ProductTest
@RequiresEnvironment(HiveIcebergRedirectionsEnvironment.class)
@TestGroup.HiveIcebergRedirections
class TestIcebergHiveViewsCompatibility
{
    @Test
    void testIcebergHiveViewsCompatibility(HiveIcebergRedirectionsEnvironment env)
            throws Exception
    {
        try {
            // ensure clean env
            cleanup(env);

            List<Row> hivePreexistingTables = env.executeTrino("SHOW TABLES FROM hive.default").getRows();
            List<Row> icebergPreexistingTables = env.executeTrino("SHOW TABLES FROM iceberg.default").getRows();

            env.executeTrinoUpdate("CREATE TABLE hive.default.hive_table AS SELECT 1 bee");
            env.executeTrinoUpdate("CREATE TABLE iceberg.default.iceberg_table AS SELECT 2 snow");

            // Create views using session context for USE statements
            // Note: With bi-directional redirections enabled (hive.iceberg-catalog-name=iceberg and
            // iceberg.hive-catalog-name=hive), unqualified table references work across catalog boundaries
            // because the catalogs redirect to each other automatically.
            env.executeTrinoInSession(session -> {
                session.executeUpdate("USE hive.default"); // for sake of unqualified table references
                session.executeUpdate("CREATE VIEW hive.default.hive_view_qualified_hive AS SELECT * FROM hive.default.hive_table");
                session.executeUpdate("CREATE VIEW hive.default.hive_view_unqualified_hive AS SELECT * FROM hive_table");
                session.executeUpdate("CREATE VIEW hive.default.hive_view_qualified_iceberg AS SELECT * FROM iceberg.default.iceberg_table");
                // With redirections enabled, this now succeeds (iceberg_table is redirected to iceberg catalog)
                session.executeUpdate("CREATE VIEW hive.default.hive_view_unqualified_iceberg AS SELECT * FROM iceberg_table");
            });

            env.executeTrinoInSession(session -> {
                session.executeUpdate("USE iceberg.default"); // for sake of unqualified table references
                session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_qualified_hive AS SELECT * FROM hive.default.hive_table");
                // With redirections enabled, this now succeeds (hive_table is redirected to hive catalog)
                session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_unqualified_hive AS SELECT * FROM hive_table");
                session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_qualified_iceberg AS SELECT * FROM iceberg.default.iceberg_table");
                session.executeUpdate("CREATE VIEW iceberg.default.iceberg_view_unqualified_iceberg AS SELECT * FROM iceberg_table");
            });

            // both hive and iceberg catalogs should list all the tables and views.
            // With bi-directional redirections, both unqualified views are now created successfully
            List<Row> newlyCreated = List.of(
                    row("hive_table"),
                    row("iceberg_table"),
                    row("hive_view_qualified_hive"),
                    row("hive_view_unqualified_hive"),
                    row("hive_view_qualified_iceberg"),
                    row("hive_view_unqualified_iceberg"),
                    row("iceberg_view_qualified_hive"),
                    row("iceberg_view_unqualified_hive"),
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

            // try to access all views via hive catalog
            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_view_unqualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_view_unqualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM hive.default.iceberg_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM hive.default.iceberg_view_unqualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM hive.default.iceberg_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM hive.default.iceberg_view_unqualified_iceberg")).containsOnly(row(2));

            // try to access all views via iceberg catalog
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.hive_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.hive_view_unqualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.hive_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.hive_view_unqualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.iceberg_view_qualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.iceberg_view_unqualified_hive")).containsOnly(row(1));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.iceberg_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(env.executeTrino("SELECT * FROM iceberg.default.iceberg_view_unqualified_iceberg")).containsOnly(row(2));
        }
        finally {
            cleanup(env);
        }
    }

    @Test
    void testViewReferencingHiveAndIcebergTables(HiveIcebergRedirectionsEnvironment env)
    {
        QueryResult expectedIcebergRows = env.executeTrino("""
                SELECT
                  true,
                  1,
                  BIGINT '1',
                  REAL '1e0',
                  1e0,
                  DECIMAL '13.1',
                  DECIMAL '123456789123456.123456789',
                  VARCHAR 'abc',
                  X'abcd',
                  DATE '2005-09-10',
                  0
                """);

        try {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table_actual_data");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table");
            env.executeHiveUpdate("DROP VIEW IF EXISTS hive_iceberg_view");

            String icebergTableData = """
                    SELECT
                      true a_boolean,
                      1 an_integer,
                      BIGINT '1' a_bigint,
                      REAL '1e0' a_real,
                      1e0 a_double,
                      DECIMAL '13.1' a_short_decimal,
                      DECIMAL '123456789123456.123456789' a_long_decimal,
                      VARCHAR 'abc' an_unbounded_varchar,
                      X'abcd' a_varbinary,
                      DATE '2005-09-10' a_date,
                      0 a_last_column
                    """;
            env.executeTrinoUpdate("CREATE TABLE iceberg.default.view_iceberg_table_actual_data AS " + icebergTableData);
            env.executeTrinoUpdate("CREATE TABLE iceberg.default.view_iceberg_table AS TABLE iceberg.default.view_iceberg_table_actual_data");
            env.executeHiveUpdate("""
                    CREATE VIEW hive_iceberg_view AS
                    SELECT view_iceberg_table.*, r_regionkey, r_name
                    FROM view_iceberg_table
                    JOIN region ON an_integer = r_regionkey
                    """);

            String tableDescription = env.executeHive("SHOW CREATE TABLE default.view_iceberg_table_actual_data").getRows().stream()
                    .map(row -> (String) row.getValue(0))
                    .collect(joining());
            String location = extractMatch(tableDescription, "LOCATION\\s*'(?<location>[^']+)'");
            String metadataLocation = extractMatch(tableDescription, "'metadata_location'='(?<location>[^']+\\.metadata\\.json)'");

            env.executeTrinoUpdate("DROP TABLE iceberg.default.view_iceberg_table");
            env.executeHiveUpdate(
                    "CREATE EXTERNAL TABLE default.view_iceberg_table (dummy_column int) " +
                            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
                            "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.FileInputFormat' " +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
                            "LOCATION '" + location + "' " +
                            "TBLPROPERTIES ('table_type'='iceberg', 'metadata_location'='" + metadataLocation + "')");

            assertThat(env.executeTrino("TABLE iceberg.default.view_iceberg_table"))
                    .containsOnly(expectedIcebergRows.getRows().toArray(Row[]::new));

            assertThatThrownBy(() -> env.executeHive("SELECT * FROM hive_iceberg_view"))
                    .rootCause()
                    .hasMessageContaining("SemanticException")
                    .hasMessageContaining("Invalid column reference 'an_integer' in definition of VIEW hive_iceberg_view");
            assertThat(env.executeTrino("SELECT * FROM hive.default.hive_iceberg_view"))
                    .containsOnly(
                            row(
                                    true,
                                    1,
                                    1L,
                                    1.0f,
                                    1d,
                                    new BigDecimal("13.1"),
                                    new BigDecimal("123456789123456.123456789"),
                                    "abc",
                                    new byte[] {(byte) 0xAB, (byte) 0xCD},
                                    Date.valueOf(LocalDate.of(2005, 9, 10)),
                                    0,
                                    1L,
                                    "AMERICA"));
        }
        finally {
            env.executeHiveUpdate("DROP VIEW IF EXISTS hive_iceberg_view");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table_actual_data");
            env.executeHiveUpdate("DROP TABLE IF EXISTS default.view_iceberg_table");
        }
    }

    private void cleanup(HiveIcebergRedirectionsEnvironment env)
    {
        // Drop views first (they depend on tables)
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_qualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_unqualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_qualified_iceberg");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default.hive_view_unqualified_iceberg");

        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_qualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_unqualified_hive");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_qualified_iceberg");
        env.executeTrinoUpdate("DROP VIEW IF EXISTS iceberg.default.iceberg_view_unqualified_iceberg");

        // Drop tables
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.hive_table");
        env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.iceberg_table");
    }

    private static String extractMatch(String text, String pattern)
    {
        Matcher matcher = Pattern.compile(pattern).matcher(text);
        if (!matcher.find()) {
            throw new IllegalStateException("Pattern not found: " + pattern);
        }
        return matcher.group("location");
    }
}
