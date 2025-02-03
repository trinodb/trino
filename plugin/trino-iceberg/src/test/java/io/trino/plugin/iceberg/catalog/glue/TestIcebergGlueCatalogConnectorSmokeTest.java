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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.glue.v1.GlueToTrinoConverter.getStorageDescriptor;
import static io.trino.plugin.hive.metastore.glue.v1.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.v1.GlueToTrinoConverter.getTableType;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/*
 * TestIcebergGlueCatalogConnectorSmokeTest currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@TestInstance(PER_CLASS)
public class TestIcebergGlueCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName;
    private final String schemaName;
    private final AWSGlueAsync glueClient;
    private final TrinoFileSystemFactory fileSystemFactory;

    public TestIcebergGlueCatalogConnectorSmokeTest()
    {
        super(FileFormat.PARQUET);
        this.bucketName = requireEnv("S3_BUCKET");
        this.schemaName = "test_iceberg_smoke_" + randomNameSuffix();
        glueClient = AWSGlueAsyncClientBuilder.defaultClient();

        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of());
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());
        this.fileSystemFactory = new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication()), new TrinoHdfsFileSystemStats());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.file-format", format.name(),
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.default-warehouse-dir", schemaPath(),
                                "iceberg.register-table-procedure.enabled", "true",
                                "iceberg.writer-sort-buffer-size", "1MB",
                                "iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max"))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @AfterAll
    public void cleanup()
    {
        computeActual("SHOW TABLES").getMaterializedRows()
                .forEach(table -> getQueryRunner().execute("DROP TABLE " + table.getField(0)));
        getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schemaName);

        // DROP TABLES should clean up any files, but clear the directory manually to be safe
        deleteDirectory(schemaPath());
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches(format("" +
                                "\\QCREATE TABLE iceberg.%1$s.region (\n" +
                                "   regionkey bigint,\n" +
                                "   name varchar,\n" +
                                "   comment varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = 'PARQUET',\n" +
                                "   format_version = 2,\n" +
                                "   location = '%2$s/%1$s.db/region-\\E.*\\Q',\n" +
                                "   max_commit_retry = 4\n" +
                                ")\\E",
                        schemaName,
                        schemaPath()));
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }

    @Test
    void testGlueTableLocation()
    {
        try (TestTable table = newTrinoTable("test_table_location", "AS SELECT 1 x")) {
            String initialLocation = getStorageDescriptor(getGlueTable(table.getName())).orElseThrow().getLocation();
            assertThat(getStorageDescriptor(getGlueTable(table.getName())).orElseThrow().getLocation())
                    // Using startsWith because the location has UUID suffix
                    .startsWith("%s/%s.db/%s".formatted(schemaPath(), schemaName, table.getName()));

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            Table glueTable = getGlueTable(table.getName());
            assertThat(getStorageDescriptor(glueTable).orElseThrow().getLocation())
                    .isEqualTo(initialLocation);

            String newTableLocation = initialLocation + "_new";
            updateTableLocation(glueTable, newTableLocation);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3", 1);
            assertThat(getStorageDescriptor(getGlueTable(table.getName())).orElseThrow().getLocation())
                    .isEqualTo(newTableLocation);

            assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + table.getName() + "')");
            assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '" + table.getName() + "', '" + initialLocation + "')");
            assertThat(getStorageDescriptor(getGlueTable(table.getName())).orElseThrow().getLocation())
                    .isEqualTo(initialLocation);
        }
    }

    private Table getGlueTable(String tableName)
    {
        GetTableRequest request = new GetTableRequest().withDatabaseName(schemaName).withName(tableName);
        return glueClient.getTable(request).getTable();
    }

    private void updateTableLocation(Table table, String newLocation)
    {
        TableInput tableInput = new TableInput()
                .withName(table.getName())
                .withTableType(getTableType(table))
                .withStorageDescriptor(getStorageDescriptor(table).orElseThrow().withLocation(newLocation))
                .withParameters(getTableParameters(table));
        UpdateTableRequest updateTableRequest = new UpdateTableRequest()
                .withDatabaseName(schemaName)
                .withTableInput(tableInput);
        glueClient.updateTable(updateTableRequest);
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(schemaName)
                .withName(tableName);
        glueClient.deleteTable(deleteTableRequest);
        GetTableRequest getTableRequest = new GetTableRequest()
                .withDatabaseName(schemaName)
                .withName(tableName);
        assertThatThrownBy(() -> glueClient.getTable(getTableRequest))
                .isInstanceOf(EntityNotFoundException.class);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        GetTableRequest getTableRequest = new GetTableRequest()
                .withDatabaseName(schemaName)
                .withName(tableName);
        return getTableParameters(glueClient.getTable(getTableRequest).getTable())
                .get("metadata_location");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try (S3Client s3 = S3Client.create()) {
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(location)
                    .build();
            s3.listObjectsV2Paginator(listObjectsRequest).stream()
                    .forEach(listObjectsResponse -> {
                        List<String> keys = listObjectsResponse.contents().stream().map(S3Object::key).collect(toImmutableList());
                        if (!keys.isEmpty()) {
                            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                                    .bucket(bucketName)
                                    .delete(builder -> builder.objects(keys.stream()
                                            .map(key -> ObjectIdentifier.builder().key(key).build())
                                            .toList()).quiet(true))
                                    .build();
                            s3.deleteObjects(deleteObjectsRequest);
                        }
                    });

            assertThat(s3.listObjects(ListObjectsRequest.builder().bucket(bucketName).prefix(location).build()).contents()).isEmpty();
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }

    @Override
    protected String schemaPath()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }

    @Override
    protected boolean locationExists(String location)
    {
        try (S3Client s3 = S3Client.create()) {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(location)
                    .maxKeys(1)
                    .build();
            return !s3.listObjectsV2(request).contents().isEmpty();
        }
    }
}
