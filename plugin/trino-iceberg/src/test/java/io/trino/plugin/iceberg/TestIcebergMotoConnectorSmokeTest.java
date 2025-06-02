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
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.MotoContainer;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.services.glue.GlueClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.MotoContainer.MOTO_ACCESS_KEY;
import static io.trino.testing.containers.MotoContainer.MOTO_REGION;
import static io.trino.testing.containers.MotoContainer.MOTO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // Moto is not concurrency safe
public class TestIcebergMotoConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName = "test-iceberg-" + randomNameSuffix();
    private final String schemaName = "test_iceberg_smoke_" + randomNameSuffix();
    private GlueClient glueClient;

    public TestIcebergMotoConnectorSmokeTest()
    {
        super(FileFormat.PARQUET);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MotoContainer moto = closeAfterClass(new MotoContainer());
        moto.start();
        moto.createBucket(bucketName);

        glueClient = closeAfterClass(GlueClient.builder().applyMutation(moto::updateClient).build());

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.catalog.type", "glue")
                        .put("hive.metastore.glue.region", MOTO_REGION)
                        .put("hive.metastore.glue.endpoint-url", moto.getEndpoint().toString())
                        .put("hive.metastore.glue.aws-access-key", MOTO_ACCESS_KEY)
                        .put("hive.metastore.glue.aws-secret-key", MOTO_SECRET_KEY)
                        .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.region", MOTO_REGION)
                        .put("s3.endpoint", moto.getEndpoint().toString())
                        .put("s3.aws-access-key", MOTO_ACCESS_KEY)
                        .put("s3.aws-secret-key", MOTO_SECRET_KEY)
                        .put("s3.path-style-access", "true")
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                        .buildOrThrow())
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(schemaName)
                        .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                        .withSchemaProperties(Map.of("location", "'s3://%s/%s/'".formatted(bucketName, schemaName)))
                        .build())
                .build();
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

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        glueClient.deleteTable(x -> x.databaseName(schemaName).name(tableName));
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return glueClient.getTable(x -> x.databaseName(schemaName).name(tableName))
                .table().parameters().get("metadata_location");
    }

    @Override
    protected String schemaPath()
    {
        return "s3://%s/%s".formatted(bucketName, schemaName);
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.directoryExists(Location.of(location)).orElse(false);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Disabled("Moto is not concurrency safe")
    @Override
    public void testDeleteRowsConcurrently() {}

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }
}
