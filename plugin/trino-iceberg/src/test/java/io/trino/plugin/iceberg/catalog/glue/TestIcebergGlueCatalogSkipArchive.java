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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableVersionsResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.TableVersion;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@TestInstance(PER_CLASS)
public class TestIcebergGlueCatalogSkipArchive
        extends AbstractTestQueryFramework
{
    private final String schemaName = "test_iceberg_skip_archive_" + randomNameSuffix();
    private GlueClient glueClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        glueClient = GlueClient.create();
        File schemaDirectory = Files.createTempDirectory("test_iceberg").toFile();
        schemaDirectory.deleteOnExit();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "glue")
                                .put("hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath())
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @AfterAll
    public void cleanup()
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
    }

    @Test
    public void testSkipArchive()
    {
        try (TestTable table = newTrinoTable("test_skip_archive", "(col int)")) {
            List<TableVersion> tableVersionsBeforeInsert = getTableVersions(schemaName, table.getName());
            assertThat(tableVersionsBeforeInsert).hasSize(1);
            String versionIdBeforeInsert = getOnlyElement(tableVersionsBeforeInsert).versionId();

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);

            // Verify count of table versions isn't increased, but version id is changed
            List<TableVersion> tableVersionsAfterInsert = getTableVersions(schemaName, table.getName());
            assertThat(tableVersionsAfterInsert).hasSize(1);
            String versionIdAfterInsert = getOnlyElement(tableVersionsAfterInsert).versionId();
            assertThat(versionIdBeforeInsert).isNotEqualTo(versionIdAfterInsert);
        }
    }

    @Test
    public void testNotRemoveExistingArchive()
    {
        try (TestTable table = newTrinoTable("test_remove_archive", "(col int)")) {
            List<TableVersion> tableVersionsBeforeInsert = getTableVersions(schemaName, table.getName());
            assertThat(tableVersionsBeforeInsert).hasSize(1);
            TableVersion initialVersion = getOnlyElement(tableVersionsBeforeInsert);

            // Add a new archive using Glue client
            Table glueTable = glueClient.getTable(builder -> builder.databaseName(schemaName).name(table.getName())).table();
            Map<String, String> tableParameters = new HashMap<>(glueTable.parameters());
            String metadataLocation = tableParameters.remove(METADATA_LOCATION_PROP);
            FileIO io = new ForwardingFileIo(getFileSystemFactory(getDistributedQueryRunner()).create(SESSION));
            TableMetadata metadata = TableMetadataParser.read(io, io.newInputFile(metadataLocation));
            boolean cacheTableMetadata = new IcebergGlueCatalogConfig().isCacheTableMetadata();
            TableInput tableInput = getTableInput(TESTING_TYPE_MANAGER, table.getName(), Optional.empty(), metadata, metadata.location(), metadataLocation, tableParameters, cacheTableMetadata);
            glueClient.updateTable(builder -> builder.databaseName(schemaName).tableInput(tableInput));
            assertThat(getTableVersions(schemaName, table.getName())).hasSize(2);

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);

            // Verify existing old table versions weren't removed
            List<TableVersion> tableVersionsAfterInsert = getTableVersions(schemaName, table.getName());
            assertThat(tableVersionsAfterInsert).hasSize(2).contains(initialVersion);
        }
    }

    private List<TableVersion> getTableVersions(String databaseName, String tableName)
    {
        return glueClient
                .getTableVersionsPaginator(x -> x
                        .databaseName(databaseName)
                        .tableName(tableName))
                .stream()
                .map(GetTableVersionsResponse::tableVersions)
                .flatMap(Collection::stream)
                .toList();
    }
}
