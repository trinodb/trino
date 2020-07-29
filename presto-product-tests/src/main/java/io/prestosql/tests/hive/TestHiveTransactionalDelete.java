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
package io.prestosql.tests.hive;

import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.log.Logger;
import io.prestosql.tests.hive.util.TemporaryHiveTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;

public class TestHiveTransactionalDelete
        extends HiveProductTest
{
    private static final Logger log = Logger.get(TestHiveTransactionalDelete.class);

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnHiveBasicDelete()
    {
        log.info("About to create table");
        onHive().executeQuery("CREATE TABLE create_on_hive_basic_delete (column1 INT, column2 BIGINT) STORED AS ORC TBLPROPERTIES('transactional'='true')");
        testBasicDelete("create_on_hive_basic_delete");
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnPrestoBasicDelete()
    {
        log.info("About to create table");
        onPresto().executeQuery("CREATE TABLE create_on_presto_basic_delete (column1 INTEGER, column2 BIGINT) WITH (format = 'ORC', transactional = true)");
        testBasicDelete("create_on_presto_basic_delete");
    }

    private void testBasicDelete(String tableName)
    {
        ensureTransactionalHive();
        log.info("About to insert rows");
        onHive().executeQuery(format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
        log.info("Deleting row 1 with hive");
        onHive().executeQuery(format("DELETE FROM %s WHERE column2 = 100", tableName));
        log.info("About to query row count");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column1 IN (2, 3, 4, 5)", tableName)))
                .containsOnly(row(4));
        log.info("About to perform first Presto delete");
        onPresto().executeQuery(format("DELETE FROM %s WHERE column1 = 4", tableName));
        log.info("Querying to verify first Presto delete worked");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column1 IN (2, 3, 5)", tableName)))
                .containsOnly(row(3));
        log.info("About to perform second Presto delete");
        onPresto().executeQuery(format("DELETE FROM %s WHERE column1 < 3", tableName));
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column1 = 3", tableName)))
                .containsOnly(row(1));
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnHiveMultiDelete()
    {
        onHive().executeQuery("CREATE TABLE create_on_hive_multi_delete (column1 INT, column2 BIGINT) STORED AS ORC TBLPROPERTIES('transactional'='true')");
        testMultiDelete("create_on_hive_multi_delete");
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnPrestoMultiDelete()
    {
        onPresto().executeQuery("CREATE TABLE create_on_presto_multi_delete (column1 INT, column2 BIGINT) WITH (transactional = true)");
        testMultiDelete("create_on_presto_multi_delete");
    }

    private void testMultiDelete(String tableName)
    {
        ensureTransactionalHive();
        log.info("About to insert first set of rows");
        onHive().executeQuery(format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
        log.info("About to insert second set of rows");
        onHive().executeQuery(format("INSERT INTO %s VALUES (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000)", tableName));
        log.info("About to have Hive delete row with column1 = 9");
        onHive().executeQuery(format("DELETE FROM %s WHERE column1 = 9", tableName));
        log.info("About to perform first Presto delete of row with column1 = 2 OR column1 = 3");
        onPresto().executeQuery(format("DELETE FROM %s WHERE column1 = 2 OR column1 = 3", tableName));
        log.info("Querying to verify that the Presto delete worked");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column1 IN (1, 4, 5, 6, 7, 8, 10)", tableName)))
                .containsOnly(row(7));
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnHiveTransactionalMetadataDelete()
    {
        onHive().executeQuery("CREATE TABLE create_on_hive_transactional_metadata_delete (column2 BIGINT) PARTITIONED BY (column1 INT) STORED AS ORC TBLPROPERTIES('transactional'='true')");
        testTransactionalMetadataDelete("create_on_hive_transactional_metadata_delete");
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnPrestoTransactionalMetadataDelete()
    {
        onPresto().executeQuery("CREATE TABLE create_on_presto_transactional_metadata_delete (column1 INT, column2 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column2'])");
        testTransactionalMetadataDelete("create_on_presto_transactional_metadata_delete");
    }

    private void testTransactionalMetadataDelete(String tableName)
    {
        ensureTransactionalHive();
        log.info("About to insert rows");
        onHive().executeQuery(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                tableName,
                makeInsertValues(1, 1, 20),
                makeInsertValues(2, 1, 20)));

        log.info("Deleting rows in Presto where column1 = 1");
        onPresto().executeQuery(format("DELETE FROM %s WHERE column2 = 1", tableName));
        log.info("Verifying that column1 = 1 rows are gone");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column2 = 1", tableName)))
                .containsOnly(row(0));
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnHiveNonTransactionalMetadataDelete()
    {
        onHive().executeQuery("CREATE TABLE create_on_hive_non_transactional_metadata_delete (column2 BIGINT) PARTITIONED BY (column1 INT) STORED AS ORC");
        testNonTransactionalMetadataDelete("create_on_hive_non_transactional_metadata_delete");
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnPrestoNonTransactionalMetadataDelete()
    {
        onPresto().executeQuery("CREATE TABLE create_on_presto_non_transactional_metadata_delete (column2 BIGINT, column1 INT) WITH (partitioned_by = ARRAY['column1'])");
        testNonTransactionalMetadataDelete("create_on_presto_non_transactional_metadata_delete");
    }

    private void testNonTransactionalMetadataDelete(String tableName)
    {
        log.info("About to insert rows");
        onHive().executeQuery(format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                tableName,
                makeInsertValues(1, 1, 20),
                makeInsertValues(2, 1, 20)));

        log.info("Deleting rows in Presto where column1 = 1");
        onPresto().executeQuery(format("DELETE FROM %s WHERE column1 = 1", tableName));
        log.info("Verifying that column1 = 1 rows are gone");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column1 = 1", tableName)))
                .containsOnly(row(0));
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnHiveUnpartitionedDeleteAll()
    {
        onHive().executeQuery("CREATE TABLE test_unpartitioned_create_on_hive_delete_all (column1 INT, column2 BIGINT) STORED AS ORC TBLPROPERTIES('transactional'='true')");
        testUnpartitionedDeleteAll("test_unpartitioned_create_on_hive_delete_all");
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnPrestoUnpartitionedDeleteAll()
    {
        onPresto().executeQuery("CREATE TABLE test_unpartitioned_create_on_presto_delete_all (column1 INT, column2 BIGINT) WITH (transactional = true)");
        testUnpartitionedDeleteAll("test_unpartitioned_create_on_presto_delete_all");
    }

    private void testUnpartitionedDeleteAll(String tableName)
    {
        ensureTransactionalHive();
        onHive().executeQuery(format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
        log.info("Deleting all rows in Presto");
        onPresto().executeQuery("DELETE FROM " + tableName);
        log.info("Verifying that all rows are gone");
        assertThat(onPresto().executeQuery("SELECT COUNT(*) FROM " + tableName))
                .containsOnly(row(0));
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnHiveMultiColumnDelete()
    {
        onHive().executeQuery("CREATE TABLE create_on_hive_multi_column_delete (column1 INT, column2 BIGINT) STORED AS ORC TBLPROPERTIES('transactional'='true')");
        testMultiColumnDelete("create_on_hive_multi_column_delete");
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnPrestoMultiColumnDelete()
    {
        onPresto().executeQuery("CREATE TABLE create_on_presto_multi_column_delete (column1 INT, column2 BIGINT) WITH (transactional = true)");
        testMultiColumnDelete("create_on_presto_multi_column_delete");
    }

    private void testMultiColumnDelete(String tableName)
    {
        ensureTransactionalHive();
        log.info("About to insert rows");
        onHive().executeQuery(format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
        String where = " WHERE column1 >= 2 AND column2 <= 400";
        log.info("About to perform Presto delete of row %s", where);
        onPresto().executeQuery(format("DELETE FROM %s %s", tableName, where));
        log.info("Querying to verify that the Presto delete worked");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %S WHERE column1 IN (1, 5)", tableName)))
                .containsOnly(row(2));
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnHivePartitionAndRowsDelete()
    {
        onHive().executeQuery("CREATE TABLE create_on_hive_partition_and_rows_delete" +
                " (column2 BIGINT) PARTITIONED BY (column1 INT) STORED AS ORC TBLPROPERTIES('transactional'='true')");
        testPartitionAndRowsDelete("create_on_hive_partition_and_rows_delete");
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testCreateOnPrestoPartitionAndRowsDelete()
    {
        onPresto().executeQuery("CREATE TABLE create_on_presto_partition_and_rows_delete" +
                " (column2 BIGINT, column1 INT) WITH (transactional = true, partitioned_by = ARRAY['column1'])");
        testPartitionAndRowsDelete("create_on_presto_partition_and_rows_delete");
    }

    // Test cases with both a partition to drop and rows to delete
    private void testPartitionAndRowsDelete(String tableName)
    {
        ensureTransactionalHive();
        log.info("About to insert rows");
        onHive().executeQuery(format("INSERT INTO %s (column1, column2) VALUES (1, 100), (1, 200), (2, 300), (2, 400), (2, 500)", tableName));
        String where = " WHERE column1 = 2 OR column2 = 200";
        log.info("About to delete of rows %s", where);
        onPresto().executeQuery(format("DELETE FROM %s %s", tableName, where));
        assertThat(onPresto().executeQuery(format("SELECT column2 FROM %s", tableName)))
                .containsOnly(row(100));
        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testFailAcidBeforeHive3()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("This tests behavior of ACID table before Hive 3 ");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable("test_fail_acid_before_hive3_" + randomTableSuffix())) {
            String tableName = table.getName();
            onHive().executeQuery("" +
                    "CREATE TABLE " + tableName + "(a bigint) " +
                    "CLUSTERED BY(a) INTO 4 BUCKETS " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='true')");

            assertThat(() -> onPresto().executeQuery("SELECT * FROM " + tableName))
                    .failsWithMessage("Failed to open transaction. Transactional tables support requires Hive metastore version at least 3.0");
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testPartitionedInsertAndRowLevelDelete()
    {
        ensureTransactionalHive();
        String tableName = "transactional_row_delete";
        onPresto().executeQuery(format("CREATE TABLE %s (column2 INT, column1 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column1'])", tableName));
        log.info("About to insert rows");
        onHive().executeQuery(format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                tableName,
                makeInsertValues(1, 1, 20),
                makeInsertValues(2, 1, 20)));
        onHive().executeQuery(format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                tableName,
                makeInsertValues(1, 21, 40),
                makeInsertValues(2, 21, 40)));
        log.info("Finished inserting rows");

        log.info("Verifying that there are 40 rows with column2 > 10 AND column2 <= 30");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column2 > 10 AND column2 <= 30", tableName)))
                .containsOnly(row(40));

        log.info("Deleting rows in Presto where column2 > 10 AND column2 <= 30");
        onPresto().executeQuery(format("DELETE FROM %s WHERE column2 > 10 AND column2 <= 30", tableName));
        log.info("Finished delete");

        log.info("Verifying that column2 > 10 AND column2 <= 30 rows are gone");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s WHERE column2 > 10 AND column2 <= 30", tableName)))
                .containsOnly(row(0));

        log.info("Verifying that there are 40 rows left");
        assertThat(onPresto().executeQuery(format("SELECT COUNT(*) FROM %s", tableName)))
                .containsOnly(row(40));

        onPresto().executeQuery("DROP TABLE " + tableName);
    }

    private String makeInsertValues(int col1Value, int col2First, int col2Last)
    {
        checkArgument(col2First <= col2Last, "The first value %s must be less or equal to the last %s", col2First, col2Last);
        return IntStream.rangeClosed(col2First, col2Last).mapToObj(i -> format("(%s, %s)", col1Value, i)).collect(Collectors.joining(", "));
    }

    private void ensureTransactionalHive()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }
    }

    // A method used only during IDE test debugging to keep the docker containers alive
    private void testUninterruptibly(Runnable runnable)
    {
        try {
            runnable.run();
        }
        catch (Throwable e) {
            log.error("Saw exception %s", e, e);
            Uninterruptibles.sleepUninterruptibly(Duration.ofDays(1));
        }
        Uninterruptibles.sleepUninterruptibly(Duration.ofDays(1));
    }
}
