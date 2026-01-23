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
package io.trino.plugin.iceberg.catalog.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

final class TestIcebergBigLakeMetastoreConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final String SCHEMA = "test_iceberg_biglake_" + randomNameSuffix();
    private static final String GCP_STORAGE_BUCKET = requireEnv("GCP_STORAGE_BUCKET");
    private static final byte[] GCS_JSON_KEY_BYTES = Base64.getDecoder().decode(requireEnv("GCP_CREDENTIALS_KEY"));
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    public TestIcebergBigLakeMetastoreConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".json");
        gcpCredentialsFile.toFile().deleteOnExit();
        Files.write(gcpCredentialsFile, GCS_JSON_KEY_BYTES);
        String projectId = OBJECT_MAPPER.readTree(GCS_JSON_KEY_BYTES).get("project_id").asText();

        return IcebergQueryRunner.builder(SCHEMA)
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("iceberg.unique-table-location", "false")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", "gs://" + GCP_STORAGE_BUCKET)
                .addIcebergProperty("iceberg.rest-catalog.security", "GOOGLE")
                .addIcebergProperty("iceberg.rest-catalog.google-project-id", projectId)
                .addIcebergProperty("iceberg.rest-catalog.view-endpoints-enabled", "false")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .addIcebergProperty("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                .addIcebergProperty("fs.native-gcs.enabled", "true")
                .addIcebergProperty("gcs.json-key-file-path", gcpCredentialsFile.toString())
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(SCHEMA)
                        .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                        .withSchemaProperties(ImmutableMap.of("location", "'gs://%s/%s'".formatted(GCP_STORAGE_BUCKET, SCHEMA)))
                        .build())
                .build();
    }

    @AfterAll
    void cleanup()
            throws Exception
    {
        // Avoid DROP SCHEMA CASCADE since BigLake metastore returns null when loading a table with missing metadata file by testDropTableWithMissingMetadataFile
        // TODO Use DROP SCHEMA CASCADE once the above issue is fixed
        fileSystem.deleteDirectory(Location.of(schemaPath()));
        for (Object tableName : computeActual("SHOW TABLES IN " + SCHEMA).getOnlyColumnAsSet()) {
            dropTableFromCatalog((String) tableName);
        }
        getQueryRunner().execute("DROP SCHEMA " + SCHEMA);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        TrinoCatalog trinoCatalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
        BaseTable table = trinoCatalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return "gs://%s/%s".formatted(GCP_STORAGE_BUCKET, SCHEMA);
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
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
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
    protected void dropTableFromCatalog(String tableName)
    {
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
    }

    @Test
    @Override // Override because the location pattern differs
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("CREATE TABLE iceberg." + SCHEMA + ".region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = 2,\n" +
                        "   location = 'gs://.*'\n" +
                        "\\)");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasStackTraceContaining("Server does not support endpoint: POST /v1/{prefix}/tables/rename");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override // TODO (https://github.com/trinodb/trino/issues/27679) Enable this test once Google fixes a bug. January 2026 release will contain the fix.
    public void testRegisterTableWithComments()
    {
        abort("skipped");
    }

    @Test
    @Override // TODO Enable once timeout issue is fixed
    public void testDeleteRowsConcurrently()
    {
        abort("skipped");
    }

    @Test
    @Override // Override because BigLake metastore requires table location to start with the prefix with the table name without following spaces
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        String tableName = "test_create_table_with_trailing_space_" + randomNameSuffix();
        String tableLocationWithoutTrailingSpace = schemaPath() + "/" + tableName;
        String tableLocationWithTrailingSpace = tableLocationWithoutTrailingSpace + " ";

        assertThat(query(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS a, 'INDIA' AS b, true AS c", tableName, tableLocationWithTrailingSpace))).failure()
                .hasMessage("Failed to create transaction")
                .hasStackTraceContaining("Malformed request: The table `location` property must point to a location in the catalog.");
    }

    @Test
    @Override // BigLake metastore requires table location to start with the prefix with the table name
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("Failed to register table")
                .hasStackTraceContaining("does not start with the expected prefix");
    }

    @Test
    @Override // BigLake metastore requires table location to start with the prefix with the table name
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("Failed to register table")
                .hasStackTraceContaining("does not start with the expected prefix");
    }

    @Test
    @Override // BigLake metastore doesn't show a table if its metadata file is missing
    public void testDropTableWithMissingMetadataFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_metadata_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        Location metadataLocation = Location.of(getMetadataLocation(tableName));

        // Delete current metadata file
        fileSystem.deleteFile(metadataLocation);
        assertThat(fileSystem.newInputFile(metadataLocation).exists())
                .describedAs("Current metadata file should not exist")
                .isFalse();

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    @Override // BigLake metastore doesn't show a table if its location is missing
    public void testDropTableWithNonExistentTableLocation()
            throws Exception
    {
        String tableName = "test_drop_table_with_non_existent_table_location_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'POLAND')", 1);

        Location tableLocation = Location.of(getTableLocation(tableName));

        // Delete table location
        fileSystem.deleteDirectory(tableLocation);
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should not exist")
                .isFalse();

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    private List<String> listFiles(Location location)
            throws IOException
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        FileIterator iterator = fileSystem.listFiles(location);
        while (iterator.hasNext()) {
            builder.add(iterator.next().location().toString());
        }
        return builder.build();
    }
}
