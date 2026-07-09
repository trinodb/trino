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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.BaseSharedMetastoreTest;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getConnectorService;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSharedGlueMetastore
        extends BaseSharedMetastoreTest
{
    private static final Logger LOG = Logger.get(TestSharedGlueMetastore.class);
    private static final String HIVE_CATALOG = "hive";

    private String schemaLocation;
    private GlueHiveMetastore glueMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(tpchSchema)
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(tpchSchema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-shared-glue-metastore-" + randomNameSuffix();
        floci.createBucket(bucketName);
        schemaLocation = "s3://%s/%s".formatted(bucketName, tpchSchema);

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                "iceberg",
                ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                        .put("fs.s3.enabled", "true")
                        .putAll(floci.s3AndGlueProperties())
                        .buildOrThrow());
        queryRunner.createCatalog(
                "iceberg_with_redirections",
                "iceberg",
                ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                        .put("iceberg.hive-catalog-name", "hive")
                        .put("fs.s3.enabled", "true")
                        .putAll(floci.s3AndGlueProperties())
                        .buildOrThrow());

        glueMetastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog(
                HIVE_CATALOG,
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                        .put("fs.s3.enabled", "true")
                        .putAll(floci.s3AndGlueProperties())
                        .buildOrThrow());
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                        .put("hive.iceberg-catalog-name", "iceberg")
                        .put("fs.s3.enabled", "true")
                        .putAll(floci.s3AndGlueProperties())
                        .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA " + tpchSchema + " WITH (location = '" + schemaLocation + "')");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, icebergSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));
        queryRunner.execute("CREATE SCHEMA " + testSchema + " WITH (location = '" + schemaLocation + "')");

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        try {
            if (glueMetastore != null) {
                glueMetastore.dropDatabase(tpchSchema, false);
                glueMetastore.dropDatabase(testSchema, false);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s or %s", tpchSchema, testSchema);
        }
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";

        return format(expectedHiveCreateSchema, catalogName, tpchSchema, schemaLocation);
    }

    @Override
    protected String getExpectedIcebergCreateSchema(String catalogName)
    {
        String expectedIcebergCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = '%s'\n" +
                ")";
        return format(expectedIcebergCreateSchema, catalogName, tpchSchema, schemaLocation);
    }
}
