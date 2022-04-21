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

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_PARTITIONING;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.TestGroups.TRINO_JDBC;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_MATCH;
import static io.trino.tests.product.hive.util.TableLocationUtils.getTableLocation;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestSyncPartitionMetadata
        extends ProductTest
{
    @BeforeTestWithContext
    public void setUp()
    {
        onHdfs("-rm -f -r " + schemaLocation());
        onHdfs("-mkdir -p " + schemaLocation());
    }

    @AfterTestWithContext
    public void tearDown()
    {
        onHdfs("-rm -f -r " + schemaLocation());
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testAddPartition()
    {
        String tableName = "test_sync_partition_metadata_add_partition";
        prepare(tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD')");
        assertPartitions(tableName, row("a", "1"), row("b", "2"), row("f", "9"));
        assertQueryFailure(() -> onTrino().executeQuery("SELECT payload, col_x, col_y FROM " + tableName + " ORDER BY 1, 2, 3 ASC"))
                .hasMessageMatching(format(".*Partition location does not exist: .*%s/col_x=b/col_y=2", tableLocation(tableName)));
        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testAddPartitionContainingCharactersThatNeedUrlEncoding()
    {
        String tableName = "test_sync_partition_metadata_add_partition_urlencode";
        String mirrorTableName = "test_sync_partition_metadata_add_partition_urlencode_mirror";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + mirrorTableName);

        onTrino().executeQuery(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                tableName));
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1024, '2022-02-01', '19:00:15'), (1024, '2022-01-17', '20:00:12')");
        String sharedTableLocation = getTableLocation(tableName, 2);
        // avoid dealing with the intricacies of adding content on the file system level
        // and possibly url encoding the file path by using
        // an external table which mirrors the previously created table
        onTrino().executeQuery(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                mirrorTableName,
                sharedTableLocation));
        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + mirrorTableName + "', 'ADD')");

        assertPartitions(tableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));
        assertPartitions(mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));

        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (2048, '2022-04-04', '16:59:13')");
        assertPartitions(tableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"), row("2022-04-04", "16:59:13"));
        assertPartitions(mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + mirrorTableName + "', 'ADD')");
        assertPartitions(mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"), row("2022-04-04", "16:59:13"));

        cleanup(mirrorTableName);
        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testDropPartition()
    {
        String tableName = "test_sync_partition_metadata_drop_partition";
        prepare(tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'DROP')");
        assertPartitions(tableName, row("a", "1"));
        assertData(tableName, row(1, "a", "1"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testDropPartitionContainingCharactersThatNeedUrlEncoding()
    {
        String tableName = "test_sync_partition_metadata_drop_partition_urlencode";
        String mirrorTableName = "test_sync_partition_metadata_drop_partition_urlencode_mirror";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + mirrorTableName);

        onTrino().executeQuery(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                tableName));
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1024, '2022-01-17', '20:00:12') , (4096, '2022-01-18', '10:40:16')");

        // avoid dealing with the intricacies of adding/removing content on the file system level
        // and possibly url encoding the file path by using
        // an external table which mirrors the previously created table
        String sharedTableLocation = getTableLocation(tableName, 2);
        onTrino().executeQuery(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                mirrorTableName,
                sharedTableLocation));
        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + mirrorTableName + "', 'ADD')");

        assertPartitions(tableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));
        assertPartitions(mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));

        // remove a partition from the shared table location
        onTrino().executeQuery("DELETE FROM " + tableName + " WHERE col_date = '2022-01-17' AND col_time='20:00:12'");

        assertPartitions(tableName, row("2022-01-18", "10:40:16"));
        assertPartitions(mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + mirrorTableName + "', 'DROP')");
        assertPartitions(mirrorTableName, row("2022-01-18", "10:40:16"));

        cleanup(mirrorTableName);
        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testFullSyncPartition()
    {
        String tableName = "test_sync_partition_metadata_add_drop_partition";
        prepare(tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(tableName, row("a", "1"), row("f", "9"));
        assertData(tableName, row(1, "a", "1"), row(42, "f", "9"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testInvalidSyncMode()
    {
        String tableName = "test_repair_invalid_mode";
        prepare(tableName);

        assertQueryFailure(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'INVALID')"))
                .hasMessageMatching("Query failed (.*): Invalid partition metadata sync mode: INVALID");

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(tableName);
        String tableLocation = tableLocation(tableName);

        String dataSource = generateOrcFile();
        onHdfs(format("-mkdir -p %s/col_x=h/col_Y=11", tableLocation));
        onHdfs(format("-cp %s %s/col_x=h/col_Y=11", dataSource, tableLocation));

        onHdfs(format("-mkdir -p %s/COL_X=UPPER/COL_Y=12", tableLocation));
        onHdfs(format("-cp %s %s/COL_X=UPPER/COL_Y=12", dataSource, tableLocation));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL', false)");
        assertPartitions(tableName, row("UPPER", "12"), row("a", "1"), row("f", "9"), row("g", "10"), row("h", "11"));
        assertData(tableName, row(1, "a", "1"), row(42, "UPPER", "12"), row(42, "f", "9"), row(42, "g", "10"), row(42, "h", "11"));
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testConflictingMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        String tableLocation = tableLocation(tableName);
        prepare(tableName);
        String dataSource = generateOrcFile();
        // this conflicts with a partition that already exits in the metastore
        onHdfs(format("-mkdir -p %s/COL_X=a/cOl_y=1", tableLocation));
        onHdfs(format("-cp %s %s/COL_X=a/cOl_y=1", dataSource, tableLocation));

        assertThatThrownBy(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD', false)"))
                .hasMessageContaining(format("One or more partitions already exist for table 'default.%s'", tableName));
        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    private String tableLocation(String tableName)
    {
        return schemaLocation() + '/' + tableName;
    }

    protected abstract String schemaLocation();

    private void prepare(String tableName)
    {
        String tableLocation = tableLocation(tableName);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHdfs("-rm -f -r " + tableLocation);
        onHdfs("-mkdir -p " + tableLocation);

        onHive().executeQuery("CREATE TABLE " + tableName + " (payload bigint) PARTITIONED BY (col_x string, col_y string) STORED AS ORC LOCATION '" + tableLocation + "'");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1, 'a', '1'), (2, 'b', '2')");

        String filePath = generateOrcFile();

        // remove partition col_x=b/col_y=2
        onHdfs(format("-rm -f -r %s/col_x=b/col_y=2", tableLocation));
        // add partition directory col_x=f/col_y=9 with single_int_column/data.orc file
        onHdfs(format("-mkdir -p %s/col_x=f/col_y=9", tableLocation));
        onHdfs(format("-cp %s %s/col_x=f/col_y=9", filePath, tableLocation));

        // should only be picked up when not in case sensitive mode
        onHdfs(format("-mkdir -p %s/COL_X=g/col_y=10", tableLocation));
        onHdfs(format("-cp %s %s/COL_X=g/col_y=10", filePath, tableLocation));

        // add invalid partition path
        onHdfs(format("-mkdir -p %s/col_x=d", tableLocation));
        onHdfs(format("-mkdir -p %s/col_y=3/col_x=h", tableLocation));
        onHdfs(format("-mkdir -p %s/col_y=3", tableLocation));
        onHdfs(format("-mkdir -p %s/xyz", tableLocation));

        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    // Drop and create a table. Then, return single ORC file path
    private String generateOrcFile()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS single_int_column");
        onTrino().executeQuery("CREATE TABLE single_int_column (payload bigint) WITH (format = 'ORC')");
        onTrino().executeQuery("INSERT INTO single_int_column VALUES (42)");
        return getOnlyElement(onTrino().executeQuery("SELECT \"$path\" FROM single_int_column").row(0)).toString();
    }

    private void onHdfs(String command)
    {
        onHive().executeQuery("dfs " + command);
    }

    private static void cleanup(String tableName)
    {
        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    private static void assertPartitions(String tableName, QueryAssert.Row... rows)
    {
        QueryResult partitionListResult = onTrino().executeQuery("SELECT * FROM \"" + tableName + "$partitions\" ORDER BY 1, 2");
        assertThat(partitionListResult).containsExactlyInOrder(rows);
    }

    private static void assertData(String tableName, QueryAssert.Row... rows)
    {
        QueryResult dataResult = onTrino().executeQuery("SELECT payload, col_x, col_y FROM " + tableName + " ORDER BY 1, 2, 3 ASC");
        assertThat(dataResult).containsExactlyInOrder(rows);
    }
}
