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

import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.hive.util.TableLocationUtils.getTableLocation;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Please add @Test with groups in the subclass
 */
public abstract class BaseTestSyncPartitionMetadata
        extends ProductTest
{
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

    public void testDropPartition()
    {
        String tableName = "test_sync_partition_metadata_drop_partition";
        prepare(tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'DROP')");
        assertPartitions(tableName, row("a", "1"));
        assertData(tableName, row(1, "a", "1"));

        cleanup(tableName);
    }

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

    public void testFullSyncPartition()
    {
        String tableName = "test_sync_partition_metadata_add_drop_partition";
        prepare(tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(tableName, row("a", "1"), row("f", "9"));
        assertData(tableName, row(1, "a", "1"), row(42, "f", "9"));

        cleanup(tableName);
    }

    public void testInvalidSyncMode()
    {
        String tableName = "test_repair_invalid_mode";
        prepare(tableName);

        assertQueryFailure(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'INVALID')"))
                .hasMessageMatching("Query failed (.*): Invalid partition metadata sync mode: INVALID");

        cleanup(tableName);
    }

    public void testMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(tableName);
        String tableLocation = tableLocation(tableName);

        makeHdfsDirectory(format("%s/col_x=h/col_Y=11", tableLocation));
        copyOrcFileToHdfsDirectory(tableName, format("%s/col_x=h/col_Y=11", tableLocation));

        makeHdfsDirectory(format("%s/COL_X=UPPER/COL_Y=12", tableLocation));
        copyOrcFileToHdfsDirectory(tableName, format("%s/COL_X=UPPER/COL_Y=12", tableLocation));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL', false)");
        assertPartitions(tableName, row("UPPER", "12"), row("a", "1"), row("f", "9"), row("g", "10"), row("h", "11"));
        assertData(tableName, row(1, "a", "1"), row(42, "UPPER", "12"), row(42, "f", "9"), row(42, "g", "10"), row(42, "h", "11"));
    }

    public void testConflictingMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        String tableLocation = tableLocation(tableName);
        prepare(tableName);
        // this conflicts with a partition that already exits in the metastore
        makeHdfsDirectory(format("%s/COL_X=a/cOl_y=1", tableLocation));
        copyOrcFileToHdfsDirectory(tableName, format("%s/COL_X=a/cOl_y=1", tableLocation));

        assertThatThrownBy(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD', false)"))
                .hasMessageContaining(format("One or more partitions already exist for table 'default.%s'", tableName));
        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    public void testSyncPartitionMetadataWithNullArgument()
    {
        assertQueryFailure(() -> onTrino().executeQuery("CALL system.sync_partition_metadata(NULL, 'page_views', 'ADD')"))
                .hasMessageMatching(".*schema_name cannot be null.*");
        assertQueryFailure(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('web', NULl, 'ADD')"))
                .hasMessageMatching(".*table_name cannot be null.*");
        assertQueryFailure(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('web', 'page_views', NULL)"))
                .hasMessageMatching(".*mode cannot be null.*");
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
        removeHdfsDirectory(tableLocation);

        createTable(tableName, tableLocation);
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1, 'a', '1'), (2, 'b', '2')");

        // remove partition col_x=b/col_y=2
        removeHdfsDirectory(format("%s/col_x=b/col_y=2", tableLocation));
        // add partition directory col_x=f/col_y=9 with single_int_column/data.orc file
        makeHdfsDirectory(format("%s/col_x=f/col_y=9", tableLocation));
        copyOrcFileToHdfsDirectory(tableName, format("%s/col_x=f/col_y=9", tableLocation));

        // should only be picked up when not in case sensitive mode
        makeHdfsDirectory(format("%s/COL_X=g/col_y=10", tableLocation));
        copyOrcFileToHdfsDirectory(tableName, format("%s/COL_X=g/col_y=10", tableLocation));

        // add invalid partition path
        makeHdfsDirectory(format("%s/col_x=d", tableLocation));
        makeHdfsDirectory(format("%s/col_y=3/col_x=h", tableLocation));
        makeHdfsDirectory(format("%s/col_y=3", tableLocation));
        makeHdfsDirectory(format("%s/xyz", tableLocation));

        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    protected abstract void createTable(String tableName, String location);

    protected abstract void removeHdfsDirectory(String path);

    protected abstract void makeHdfsDirectory(String path);

    protected abstract void copyOrcFileToHdfsDirectory(String tableName, String targetDirectory);

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
