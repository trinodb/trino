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
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;

public class TestDeltaLakeFlushMetadataCacheProcedure
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "delta-lake-test-flush-metadata-cache";

    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(BUCKET_NAME, HIVE3_IMAGE);
        hiveMinioDataLake.start();
        metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .build());

        return createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                "default",
                ImmutableMap.of("hive.metastore-cache-ttl", "10m"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        metastore = null;
    }

    @Test
    public void testFlushMetadataCache()
    {
        assertUpdate("CREATE SCHEMA cached WITH (location = 's3://" + BUCKET_NAME + "/cached')");
        assertUpdate("CREATE TABLE cached.cached AS SELECT * FROM tpch.tiny.nation", 25);

        // Verify that column cache is flushed
        // Fill caches
        assertQuerySucceeds("SELECT name, regionkey FROM cached.cached");

        // Verify that table cache is flushed
        String showTablesSql = "SHOW TABLES FROM cached";
        // Fill caches
        assertQuery(showTablesSql, "VALUES 'cached'");

        // Rename table outside Trino
        metastore.renameTable("cached", "cached", "cached", "renamed");

        // Should still return old table name from cache
        assertQuery(showTablesSql, "VALUES 'cached'");

        // Should return new table name after cache flush
        assertUpdate("CALL system.flush_metadata_cache(schema_name => 'cached', table_name => 'cached')");
        assertQuery(showTablesSql, "VALUES 'renamed'");

        // Verify that schema cache is flushed
        String showSchemasSql = "SHOW SCHEMAS FROM delta_lake";
        // Fill caches
        assertQuery(showSchemasSql, "VALUES ('cached'), ('information_schema'), ('default')");

        // Drop a table and a schema outside Trino
        metastore.dropTable("cached", "renamed", false);
        metastore.dropDatabase("cached", false);

        // Should still return old schemas from cache
        assertQuery(showSchemasSql, "VALUES ('cached'), ('information_schema'), ('default')");

        // Should not return the old schema name after cache flush
        assertUpdate("CALL system.flush_metadata_cache()");
        assertQuery(showSchemasSql, "VALUES ('information_schema'), ('default')");
    }

    @Test
    public void testFlushMetadataCacheTableNotFound()
    {
        assertQueryFails(
                "CALL system.flush_metadata_cache(schema_name => 'test_not_existing_schema', table_name => 'test_not_existing_table')",
                "Table 'test_not_existing_schema.test_not_existing_table' not found");
    }
}
