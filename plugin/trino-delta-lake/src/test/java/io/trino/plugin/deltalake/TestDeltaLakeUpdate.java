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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

// An Update test is run against all supported file systems from AbstractTestDeltaLakeIntegrationSmokeTest#testUpdate
public class TestDeltaLakeUpdate
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "default";
    private final String bucketName;

    public TestDeltaLakeUpdate()
    {
        this.bucketName = "test-delta-lake-connector-test-" + randomNameSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(!new ParquetWriterConfig().isParquetOptimizedWriterEnabled(), "This test assumes the optimized Parquet writer is disabled by default");

        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        QueryRunner queryRunner = DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());

        TpchTable.getTables().forEach(table ->
                queryRunner.execute(format("CREATE TABLE %s WITH (location = '%s') AS SELECT * FROM tpch.tiny.%1$s", table.getTableName(), getLocationForTable(table.getTableName()))));

        return queryRunner;
    }

    @Test
    public void testSimpleUpdate()
    {
        String tableName = "test_simple_update_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (location = '" + getLocationForTable(tableName) + "') " +
                        "AS VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)",
                "VALUES 5");
        assertUpdate("UPDATE " + tableName + " SET a = 42 WHERE a = 1", "VALUES 3");
        assertQuery("SELECT * FROM " + tableName, "VALUES (42, 2, 3), (42, 2, 4), (3, 2, 1), (null, null, null), (42, 1, 1)");

        assertUpdate("UPDATE " + tableName + " SET a = 42 WHERE b = 2", "VALUES 3");
        assertQuery("SELECT * FROM " + tableName, "VALUES (42, 2, 3), (42, 2, 4), (42, 2, 1), (null, null, null), (42, 1, 1)");

        assertUpdate("UPDATE " + tableName + " SET a = 0 WHERE c = 1", "VALUES 2");
        assertQuery("SELECT * FROM " + tableName, "VALUES (42, 2, 3), (42, 2, 4), (0, 2, 1), (null, null, null), (0, 1, 1)");

        assertUpdate("UPDATE " + tableName + " SET a = 1", "VALUES 5");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 3), (1, 2, 4), (1, 2, 1), (1, null, null), (1, 1, 1)");

        assertUpdate("UPDATE " + tableName + " SET b = 2", "VALUES 5");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 3), (1, 2, 4), (1, 2, 1), (1, 2, null), (1, 2, 1)");

        assertUpdate("UPDATE " + tableName + " SET c = 3", "VALUES 5");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 3), (1, 2, 3), (1, 2, 3), (1, 2, 3), (1, 2, 3)");

        assertUpdate("UPDATE " + tableName + " SET c = 0 WHERE a = 1", "VALUES 5");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 0), (1, 2, 0), (1, 2, 0), (1, 2, 0), (1, 2, 0)");

        assertUpdate("UPDATE " + tableName + " SET c = 1 WHERE b = 2", "VALUES 5");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 1), (1, 2, 1), (1, 2, 1), (1, 2, 1), (1, 2, 1)");

        assertUpdate("UPDATE " + tableName + " SET c = 2 WHERE c IS NOT NULL", "VALUES 5");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 2), (1, 2, 2), (1, 2, 2), (1, 2, 2), (1, 2, 2)");
    }

    @Test
    public void testUpdateAll()
    {
        String tableName = "test_update_all_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b) WITH (location = '" + getLocationForTable(tableName) + "') " +
                        "AS VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)",
                "VALUES 5");
        assertUpdate("UPDATE " + tableName + " SET a = a + 1", "VALUES 5");
        assertQuery("SELECT * FROM " + tableName, "VALUES (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)");
    }

    @Test
    public void testUpdateSingleRow()
    {
        String tableName = "test_update_single_row_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b) WITH (location = '" + getLocationForTable(tableName) + "') " +
                        "AS VALUES (1, 2)",
                "VALUES 1");
        assertUpdate("UPDATE " + tableName + " SET a = a + 1", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName, "VALUES (2, 2)");
    }

    @Test
    public void testUpdateNone()
    {
        String tableName = "test_update_none_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b) WITH (location = '" + getLocationForTable(tableName) + "') " +
                        "AS VALUES (1, 2)",
                "VALUES 1");
        assertUpdate("UPDATE " + tableName + " SET a = a + 1 WHERE a > 42", "VALUES 0");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2)");
    }

    @Test
    public void testUpdateOnPartitionKey()
    {
        String tableName = "test_update_on_partition_key_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (location = '" + getLocationForTable(tableName) + "', partitioned_by = ARRAY['b']) " +
                        "AS VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)",
                "VALUES 5");
        assertUpdate("UPDATE " + tableName + " SET b = 42", 5);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 42, 3), (1, 42, 4), (3, 42, 1), (null, 42, null), (1, 42, 1)");
        assertUpdate("UPDATE " + tableName + " SET b = 42", 5);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 42, 3), (1, 42, 4), (3, 42, 1), (null, 42, null), (1, 42, 1)");
        assertUpdate("UPDATE " + tableName + " SET b = 32 WHERE a IS NULL", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 42, 3), (1, 42, 4), (3, 42, 1), (null, 32, null), (1, 42, 1)");
        assertUpdate("UPDATE " + tableName + " SET a = 12, b = 5 WHERE b = 42 AND c < 3", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 42, 3), (1, 42, 4), (12, 5, 1), (null, 32, null), (12, 5, 1)");
    }

    @Test
    public void testUpdateWithPartitionKeyPredicate()
    {
        String tableName = "test_update_with_partition_key_predicate_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (location = '" + getLocationForTable(tableName) + "', partitioned_by = ARRAY['b']) " +
                        "AS VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)",
                "VALUES 5");
        assertUpdate("UPDATE " + tableName + " SET c = 42 WHERE a = 1 AND b = 2", "VALUES 2");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 42), (1, 2, 42), (3, 2, 1), (null, null, null), (1, 1, 1)");

        assertUpdate("UPDATE " + tableName + " SET a = 42 WHERE b IS NULL", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 42), (1, 2, 42), (3, 2, 1), (42, null, null), (1, 1, 1)");
    }

    @Test
    public void testUpdateNull()
    {
        String tableName = "test_update_null_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (location = '" + getLocationForTable(tableName) + "') " +
                        "AS VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)",
                "VALUES 5");
        assertUpdate("UPDATE " + tableName + " SET b = 42 WHERE a IS NULL", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, 42, null), (1, 1, 1)");

        assertUpdate("UPDATE " + tableName + " SET b = NULL WHERE a IS NULL", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)");
    }

    @Test
    public void testUpdateAllColumns()
    {
        String tableName = "test_update_all_columns_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (location = '" + getLocationForTable(tableName) + "') " +
                        "AS VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (null, null, null), (1, 1, 1)",
                "VALUES 5");
        assertUpdate("UPDATE " + tableName + " SET c = 100, b = 42, a = 0 WHERE a IS NULL", "VALUES 1");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 3), (1, 2, 4), (3, 2, 1), (0, 42, 100), (1, 1, 1)");
    }

    private String getLocationForTable(String tableName)
    {
        return format("s3://%s/%s", bucketName, tableName);
    }
}
