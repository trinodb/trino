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

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_ICEBERG_REDIRECTIONS;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestHiveReadingTheTables
        extends ProductTest
{
    private final String hiveSerdeIcebergFormatName = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";

    @BeforeTestWithContext
    public void setUp()
    {
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS iceberg.iceberg");
    }

    @AfterTestWithContext
    public void cleanUp()
    {
        onTrino().executeQuery("DROP SCHEMA iceberg.iceberg");
    }

    @Test(groups = { ICEBERG, HIVE_ICEBERG_REDIRECTIONS })
    public void testCreateTableWithHiveSupportEnabled()
    {
        String tableName = "iceberg.iceberg.test_table";
        onTrino().executeQuery("CREATE TABLE " + tableName + "(a BIGINT, b VARCHAR) WITH(hive_enabled=true)");
        try {
            assertThat(onHive().executeQuery(format("SELECT * FROM %s", "iceberg.test_table"))).containsOnly();
            assertThat(onSpark().executeQuery(format("SELECT * FROM %s", "iceberg_test.iceberg.test_table")))
                    .containsOnly();
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, HIVE_ICEBERG_REDIRECTIONS})
    public void testCreateTableWithHiveSupportDisabled()
    {
        String tableName = "iceberg.iceberg.test_table";
        onTrino().executeQuery("CREATE TABLE " + tableName + "(a BIGINT, b VARCHAR) WITH(hive_enabled=false)");
        try {
            assertQueryFailure(() -> onHive().executeQuery(format("SELECT * FROM %s", "iceberg.test_table")));
            assertThat(onSpark().executeQuery(format("SELECT * FROM %s", "iceberg_test.iceberg.test_table")))
                    .containsOnly();
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, HIVE_ICEBERG_REDIRECTIONS})
    public void testByDefaultTableShouldHasHiveEnabledSetToFalse()
    {
        String tableName = "iceberg.iceberg.test_table";
        onTrino().executeQuery("CREATE TABLE " + tableName + "(a BIGINT, b VARCHAR)");
        try {
            assertQueryFailure(() -> onHive().executeQuery(format("SELECT * FROM %s", "iceberg.test_table")));
            assertThat(onSpark().executeQuery(format("SELECT * FROM %s", "iceberg_test.iceberg.test_table")))
                    .containsOnly();
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, HIVE_ICEBERG_REDIRECTIONS})
    public void testReadingHiveCreatedIcebergTable()
    {
        String tableName = "iceberg.iceberg.test_table";
        String hiveTableName = "iceberg.test_table";

        try {
            onHive().executeQuery(
                    format("CREATE TABLE %s (id BIGINT, name string) PARTITIONED BY (dept string)", hiveTableName)
                            .concat(format("\n STORED BY '%s'", hiveSerdeIcebergFormatName)));

            assertThat(onTrino().executeQuery(
              format("SELECT * FROM %s", tableName)
            )).containsOnly();
        }
        finally {
            onHive().executeQuery("DROP TABLE " + hiveTableName);
        }
    }

    @Test(groups = {ICEBERG, HIVE_ICEBERG_REDIRECTIONS})
    public void testCreatedModifiedTableByTrinoTable()
    {
        String tableName = "iceberg.iceberg.test_table";
        String hiveTableName = "iceberg.test_table";
        String sparkTableName = "iceberg_test.iceberg.test_table";

        try {
            onTrino().executeQuery(
                    format("CREATE TABLE %s (id BIGINT, name VARCHAR, band VARCHAR) WITH (hive_enabled=true)", tableName));
            onTrino().executeQuery(
                    format("INSERT INTO %s VALUES (1, 'James', 'Metallica')", tableName));
            onTrino().executeQuery(
                    format("INSERT INTO %s VALUES (2, 'Matt', 'Trivium')", tableName));

            assertThat(onHive().executeQuery(format("SELECT * FROM %s", hiveTableName)))
                    .containsOnly(
                            row(1, "James", "Metallica"),
                            row(2, "Matt", "Trivium"));
            assertThat(onSpark().executeQuery(format("SELECT * FROM %s", sparkTableName)))
                    .containsOnly(
                            row(1, "James", "Metallica"),
                            row(2, "Matt", "Trivium"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, HIVE_ICEBERG_REDIRECTIONS})
    public void testCreatedModifiedTableByTrinoWithDifferentDataTypes()
    {
        String tableName = "iceberg.iceberg.test_table";
        String hiveTableName = "iceberg.test_table";

        onTrino().executeQuery(
                format("CREATE TABLE %s (\n", tableName)
                        .concat("col1 BOOLEAN,\n")
                        .concat("col4 INTEGER,\n")
                        .concat("col5 BIGINT,\n")
                        .concat("col6 DECIMAL(10,3),\n")
                        .concat("col7 VARCHAR,\n")
                        .concat("col9 DATE,\n")
                        .concat("col11 TIMESTAMP(6)\n")
                        .concat(") WITH (hive_enabled=true)"));
        onTrino().executeQuery(
                format("INSERT INTO %s VALUES", tableName)
                        .concat("(true, 1, CAST(4 AS BIGINT), CAST(12.0 AS DECIMAL(10, 3)), ")
                        .concat("'TEXT', CAST('2001-08-22' AS DATE),")
                        .concat("CAST('2020-06-10 15:55:23' AS TIMESTAMP(6)))"));

        try {
            assertThat(onHive().executeQuery(format("SELECT * FROM %s", hiveTableName)))
                    .containsOnly(
                            row(true, 1, 4, new BigDecimal("12.000"), "TEXT",
                                    java.sql.Date.valueOf(LocalDate.of(2001, 8, 22)),
                                    java.sql.Timestamp.valueOf(LocalDateTime.of(2020, 6, 10, 15, 55, 23, 0))));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, HIVE_ICEBERG_REDIRECTIONS})
    public void testPropertiesAndSerdeAssignedINHMS()
    {
        String enabledTableName = "iceberg.iceberg.hive_enabled";
        String onHiveEnabledTableName = "iceberg.hive_enabled";

        try {
            onTrino().executeQuery(
                    format("CREATE TABLE %s (id BIGINT, name VARCHAR, band VARCHAR) WITH (hive_enabled=true)",
                            enabledTableName));

            assertThat(onHive().executeQuery(format("DESCRIBE FORMATTED %s", onHiveEnabledTableName)))
                    .contains(
                            row("", "storage_handler     ", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"),
                            row("OutputFormat:       ", "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat", null),
                            row("SerDe Library:      ", "org.apache.iceberg.mr.hive.HiveIcebergSerDe", null),
                            row("InputFormat:        ", "org.apache.iceberg.mr.hive.HiveIcebergInputFormat", null),
                            row("", "engine.hive.enabled ", "true                "));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + enabledTableName);
        }
    }
}
