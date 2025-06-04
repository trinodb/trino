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

import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeFlushMetadataCacheProcedure
        extends AbstractTestQueryFramework
{
    private final String bucketName = "delta-lake-test-flush-metadata-cache-" + randomNameSuffix();
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Hive3MinioDataLake hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName, HIVE3_IMAGE));
        hiveMinioDataLake.start();
        metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));

        return DeltaLakeQueryRunner.builder("default")
                .addMetastoreProperties(hiveMinioDataLake.getHiveHadoop())
                .addS3Properties(hiveMinioDataLake.getMinio(), bucketName)
                .addDeltaProperty("hive.metastore-cache-ttl", "10m")
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        metastore = null;
    }

    @Test
    public void testFlushMetadataCache()
    {
        assertUpdate("CREATE SCHEMA cached WITH (location = 's3://" + bucketName + "/cached')");
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
        String showSchemasSql = "SHOW SCHEMAS FROM delta";
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
    public void testFlushMetadataCacheAfterTableCreated()
    {
        String schema = getSession().getSchema().orElseThrow();

        String location = "s3://%s/test_flush_intermediate_tmp_table".formatted(bucketName);
        assertUpdate("CREATE TABLE test_flush_intermediate_tmp_table WITH (location = '" + location + "') AS TABLE tpch.tiny.region", 5);

        // This may cause the connector to cache the fact that the table does not exist
        assertQueryFails("TABLE flush_metadata_after_table_created", "\\Qline 1:1: Table 'delta.default.flush_metadata_after_table_created' does not exist");

        metastore.renameTable(schema, "test_flush_intermediate_tmp_table", schema, "flush_metadata_after_table_created");

        // Verify cached state (we currently cache missing objects in CachingMetastore)
        assertQueryFails("TABLE flush_metadata_after_table_created", "\\Qline 1:1: Table 'delta.default.flush_metadata_after_table_created' does not exist");

        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'flush_metadata_after_table_created')");
        assertThat(query("TABLE flush_metadata_after_table_created"))
                .skippingTypesCheck() // Delta has no parametric varchar
                .matches("TABLE tpch.tiny.region");

        assertUpdate("DROP TABLE flush_metadata_after_table_created");
    }

    @Test
    public void testFlushMetadataCacheNonExistentTable()
    {
        assertUpdate("CALL system.flush_metadata_cache(schema_name => 'test_not_existing_schema', table_name => 'test_not_existing_table')");
    }
}
