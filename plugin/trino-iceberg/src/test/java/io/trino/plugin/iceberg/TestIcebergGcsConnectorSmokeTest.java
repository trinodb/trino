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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.FlociGcp;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.ORC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergGcsConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName = "test-iceberg-gcs-" + randomNameSuffix();
    private final String schema = "test_iceberg_gcs_connector_smoke_test_" + randomNameSuffix();

    private HiveMetastore metastore;

    public TestIcebergGcsConnectorSmokeTest()
    {
        super(ORC);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociGcp flociGcp = closeAfterClass(new FlociGcp());
        flociGcp.start();
        flociGcp.createBucket(bucketName);

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                        .put("fs.gcs.enabled", "true")
                        .put("gcs.auth-type", "SERVICE_ACCOUNT")
                        .put("gcs.endpoint", flociGcp.getEndpoint().toString())
                        .put("gcs.json-key", flociGcp.getServiceAccountJson())
                        .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaName(schema)
                                .withSchemaProperties(ImmutableMap.of("location", "'" + schemaPath() + "'"))
                                .build())
                .build();
        metastore = getHiveMetastore(queryRunner);
        return queryRunner;
    }

    @Override
    protected String createSchemaSql(String schema)
    {
        return format("CREATE SCHEMA %1$s WITH (location = '%2$s%1$s')", schema, schemaPath());
    }

    @Override
    protected String schemaPath()
    {
        return format("gs://%s/%s/", bucketName, schema);
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        metastore.dropTable(schema, tableName, false);
        assertThat(metastore.getTable(schema, tableName)).isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return metastore
                .getTable(schema, tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
