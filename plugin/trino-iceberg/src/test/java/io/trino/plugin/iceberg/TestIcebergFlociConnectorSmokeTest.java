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

import io.trino.filesystem.Location;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergFlociConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName = "test-iceberg-" + randomNameSuffix();
    private final String schemaName = "test_iceberg_smoke_" + randomNameSuffix();
    private GlueClient glueClient;

    public TestIcebergFlociConnectorSmokeTest()
    {
        super(FileFormat.PARQUET);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        floci.createBucket(bucketName);
        glueClient = closeAfterClass(floci.createGlueClient());

        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.catalog.type", "glue")
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .addIcebergProperty("fs.s3.enabled", "true")
                .addIcebergProperties(floci.s3AndGlueProperties())
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
    protected void dropTableFromCatalog(String tableName)
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
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }
}
