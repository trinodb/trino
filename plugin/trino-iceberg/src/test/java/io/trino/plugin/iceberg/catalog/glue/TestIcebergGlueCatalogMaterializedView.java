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
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import software.amazon.awssdk.services.glue.GlueClient;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;

public class TestIcebergGlueCatalogMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private final String schemaName = "test_iceberg_materialized_view_" + randomNameSuffix();

    private FlociS3AndGlue floci;
    private String schemaLocation;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-iceberg-glue-materialized-view-" + randomNameSuffix();
        floci.createBucket(bucketName);
        schemaLocation = "s3://%s/%s.db".formatted(bucketName, schemaName);

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.catalog.type", "glue")
                .addIcebergProperty("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .addIcebergProperty("fs.s3.enabled", "true")
                .addIcebergProperties(floci.s3AndGlueProperties())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.of())
                                .withSchemaName(schemaName)
                                .build())
                .build();
        try {
            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", ImmutableMap.<String, String>builder()
                    .put("iceberg.catalog.type", "glue")
                    .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                    .put("iceberg.materialized-views.hide-storage-table", "false")
                    .put("fs.s3.enabled", "true")
                    .putAll(floci.s3AndGlueProperties())
                    .buildOrThrow());

            queryRunner.installPlugin(createMockConnectorPlugin());
            queryRunner.createCatalog("mock", "mock");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected String getSchemaDirectory()
    {
        return schemaLocation;
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        try (GlueClient glueClient = floci.createGlueClient()) {
            return glueClient.getTable(x -> x
                            .databaseName(schemaName)
                            .name(materializedViewName))
                    .table()
                    .parameters().get(METADATA_LOCATION_PROP);
        }
    }
}
