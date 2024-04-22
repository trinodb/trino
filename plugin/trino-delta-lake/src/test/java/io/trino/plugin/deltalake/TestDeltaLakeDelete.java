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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Set;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
public class TestDeltaLakeDelete
        extends AbstractTestQueryFramework
{
    private final String bucketName = "test-delta-lake-connector-test-" + randomNameSuffix();
    private HiveMinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();

        return DeltaLakeQueryRunner.builder()
                .addMetastoreProperties(hiveMinioDataLake.getHiveHadoop())
                .addS3Properties(hiveMinioDataLake.getMinio(), bucketName)
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .build();
    }

    @Test
    public void testTargetedDeleteWhenTableIsPartitionedWithColumnContainingSpecialCharacters()
    {
        String tableName = "test_targeted_delete_with_special_characters_in_partition_key";
        assertUpdate("CREATE TABLE " + tableName + " (id, col_name) " +
                "WITH (partitioned_by = ARRAY['col_name'])  " +
                "AS VALUES " +
                "(1, 'with-hyphen'), " +
                "(2, 'with:colon'), " +
                "(3, 'with:colon'), " + // create two rows in a single file to trigger parquet file rewrite on delete
                "(4, 'with?question')", 4);
        assertQuery("SELECT count(*), count(DISTINCT \"$path\"), col_name FROM " + tableName + " GROUP BY 3", "VALUES (1, 1, 'with-hyphen'), (2, 1, 'with:colon'), (1, 1, 'with?question')");
        assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'with-hyphen'), (3, 'with:colon'), (4, 'with?question')");
        assertUpdate("DELETE FROM " + tableName, 3);
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);
    }

    @Test
    public void testTargetedDelete()
    {
        String tableName = "test_targeted_delete";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.orders", "SELECT count(*) FROM orders");
        assertUpdate("DELETE FROM " + tableName + " WHERE orderkey = 60000", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE orderkey != 60000");
    }

    @Test
    public void testDeleteDatabricksMultiFile()
    {
        testDeleteMultiFile(
                "multi_file_databricks",
                "io/trino/plugin/deltalake/testing/resources/databricks73");
    }

    @Test
    public void testDeleteOssDeltaLakeMultiFile()
    {
        testDeleteMultiFile(
                "multi_file_deltalake",
                "io/trino/plugin/deltalake/testing/resources/ossdeltalake");
    }

    private void testDeleteMultiFile(String tableName, String resourcePath)
    {
        hiveMinioDataLake.copyResources(resourcePath + "/lineitem", tableName);
        getQueryRunner().execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', 's3://%s/%s')", tableName, bucketName, tableName));

        assertQuery("SELECT count(*) FROM " + tableName, "SELECT count(*) FROM lineitem");
        assertUpdate("DELETE FROM " + tableName + " WHERE partkey % 2 = 0", "SELECT count(*) FROM lineitem WHERE partkey % 2 = 0");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM lineitem WHERE partkey % 2 = 1");
    }

    @Test
    public void testDeleteOnPartitionKey()
    {
        String tableName = "test_delete_on_partition_key";
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, p_key) WITH (partitioned_by = ARRAY['p_key']) " +
                        "AS VALUES (1, 'a'), (2, 'b'), (3, 'c'), (2, 'a'), (null, null), (1, null)",
                6);
        assertUpdate("DELETE FROM " + tableName + " WHERE p_key IS NULL", "VALUES 2");
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");
    }

    @Test
    public void testDeleteFromPartitionedTable()
    {
        String tableName = "test_delete_from_partitioned_table";
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, p_key) WITH (partitioned_by = ARRAY['p_key']) " +
                        "AS VALUES (1, 'a'), (2, 'b'), (3, 'c'), (2, 'a'), (null, null), (1, null)",
                6);
        assertUpdate("DELETE FROM " + tableName + " WHERE a = 2", "VALUES 2");
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 4");
    }

    @Test
    public void testDeleteTimestamps()
    {
        String tableName = "test_delete_timestamps";
        assertUpdate("CREATE TABLE " + tableName + " (ts) AS VALUES TIMESTAMP '2021-02-03 01:02:03.456 UTC', TIMESTAMP '2021-02-04 01:02:03.456 UTC'", 2);
        assertUpdate("DELETE FROM " + tableName + " WHERE ts = TIMESTAMP '2021-02-03 01:02:03.456 UTC'", 1);
        assertQuery("SELECT CAST(ts AS VARCHAR) FROM " + tableName, "VALUES '2021-02-04 01:02:03.456 UTC'");
    }

    @Test
    public void testDeleteOnRowType()
    {
        String tableName = "test_delete_on_row_type";
        assertUpdate("CREATE TABLE " + tableName + " (nested, a, b) AS VALUES (CAST(ROW(1, 2) AS ROW(a int, b int)), 2, 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ((1, 2), 2, 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ((2, 1), 2, 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ((1, 2), null, null)", 1);

        assertUpdate("DELETE FROM " + tableName + " WHERE a = 1", 0);
        assertUpdate("DELETE FROM " + tableName + " WHERE nested.a = 1", 3);
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 1");
    }

    @Test
    public void testDeleteAllDatabricks()
    {
        String tableName = "test_delete_all_databricks";
        Set<String> originalFiles = testDeleteAllAndReturnInitialDataLakeFilesSet(
                tableName,
                "io/trino/plugin/deltalake/testing/resources/databricks73");

        Set<String> expected = ImmutableSet.<String>builder()
                .addAll(originalFiles)
                .add(tableName + "/_delta_log/00000000000000000021.json")
                .build();
        assertThat(hiveMinioDataLake.listFiles(tableName)).containsExactlyInAnyOrder(expected.toArray(new String[0]));
    }

    @Test
    public void testDeleteAllOssDeltaLake()
    {
        String tableName = "test_delete_all_deltalake";
        hiveMinioDataLake.copyResources("io/trino/plugin/deltalake/testing/resources/ossdeltalake/customer", tableName);
        Set<String> originalFiles = ImmutableSet.copyOf(hiveMinioDataLake.listFiles(tableName));
        getQueryRunner().execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', 's3://%s/%s')", tableName, bucketName, tableName));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM customer");
        // There are `add` files in the transaction log without stats, reason why the DELETE statement on the whole table
        // performed on the basis of metadata does not return the number of deleted records
        assertUpdate("DELETE FROM " + tableName);
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        Set<String> expected = ImmutableSet.<String>builder()
                .addAll(originalFiles)
                .add(tableName + "/_delta_log/00000000000000000001.json")
                .build();
        assertThat(hiveMinioDataLake.listFiles(tableName)).containsExactlyInAnyOrder(expected.toArray(new String[0]));
    }

    private Set<String> testDeleteAllAndReturnInitialDataLakeFilesSet(String tableName, String resourcePath)
    {
        hiveMinioDataLake.copyResources(resourcePath + "/customer", tableName);
        Set<String> originalFiles = ImmutableSet.copyOf(hiveMinioDataLake.listFiles(tableName));
        getQueryRunner().execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', 's3://%s/%s')", tableName, bucketName, tableName));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM customer");
        assertUpdate("DELETE FROM " + tableName, "SELECT count(*) FROM customer");
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        return originalFiles;
    }

    @Test
    public void testStatsAfterDelete()
    {
        String tableName = "test_stats_after_delete";
        assertUpdate("CREATE TABLE " + tableName + " (a, b, c) AS VALUES (1, 3, 5), (7, 9, null), (null, null, null), (null, null, null)", 4);
        assertQuery("SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('a', null, 2.0, 0.5, null, 1, 7)," +
                        "('b', null, 2.0, 0.5, null, 3, 9)," +
                        "('c', null, 1.0, 0.75, null, 5, 5)," +
                        "(null, null, null, null, 4.0, null, null)");
        assertUpdate("DELETE FROM " + tableName + " WHERE c IS NULL", 3);
        assertQuery("SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('a', null, 1.0, 0.0, null, 1, 1)," +
                        "('b', null, 1.0, 0.0, null, 3, 3)," +
                        "('c', null, 1.0, 0.0, null, 5, 5)," +
                        "(null, null, null, null, 1.0, null, null)");
    }

    @Test
    public void testDeleteWithHiddenColumn()
    {
        String tableName = "test_delete_with_hidden_column";
        assertUpdate("CREATE TABLE " + tableName + " (a, b, c) AS VALUES (1, 3, 5), (2, 4, 6), (null, null, null), (0, 0, 0)", 4);
        assertUpdate("DELETE FROM " + tableName + " WHERE \"$file_size\" > 0", 4);
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
    }

    @Test
    public void testDeleteWithRowFilter()
    {
        String tableName = "test_delete_with_row_filter";
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " WITH (partitioned_by = ARRAY['regionkey']) " +
                        "AS SELECT nationkey, regionkey FROM tpch.tiny.nation",
                25);
        assertUpdate("DELETE FROM " + tableName + " WHERE regionkey = 4 AND nationkey < 100", "SELECT count(*) FROM nation WHERE regionkey = 4 AND nationkey < 100");
        assertQuery("SELECT * FROM " + tableName, "SELECT nationkey, regionkey FROM nation WHERE regionkey != 4 OR nationkey >= 100");
    }

    @Test
    public void testDeleteMultiplePartitionKeys()
    {
        String tableName = "test_delete_multiple_partition_keys";
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (partitioned_by = ARRAY['b', 'c']) " +
                        "AS VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)",
                5);
        assertUpdate("DELETE FROM " + tableName + " WHERE a = 1 AND c = 3", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)");
    }
}
