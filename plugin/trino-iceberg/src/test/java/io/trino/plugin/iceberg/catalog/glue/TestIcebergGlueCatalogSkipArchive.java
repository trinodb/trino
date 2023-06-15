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
import io.trino.plugin.hive.aws.AwsApiCallStats;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.util.AutoCloseableCloser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableVersionsRequest;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.TableVersion;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.paginators.GetTableVersionsPublisher;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.awsSyncPaginatedRequest;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.awsSyncRequest;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
public class TestIcebergGlueCatalogSkipArchive
        extends AbstractTestQueryFramework
{
    private final String schemaName = "test_iceberg_skip_archive_" + randomNameSuffix();
    private GlueAsyncClient glueClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        glueClient = GlueAsyncClient.create();
        File schemaDirectory = Files.createTempDirectory("test_iceberg").toFile();
        schemaDirectory.deleteOnExit();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "glue")
                                .put("iceberg.glue.skip-archive", "true")
                                .put("hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath())
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            closer.register(glueClient);
        }
        glueClient = null;
    }

    @Test
    public void testSkipArchive()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_skip_archive", "(col int)")) {
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
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_remove_archive", "(col int)")) {
            List<TableVersion> tableVersionsBeforeInsert = getTableVersions(schemaName, table.getName());
            assertThat(tableVersionsBeforeInsert).hasSize(1);
            TableVersion initialVersion = getOnlyElement(tableVersionsBeforeInsert);

            // Add a new archive using Glue client
            Table glueTable = awsSyncRequest(glueClient::getTable, GetTableRequest.builder().databaseName(schemaName).name(table.getName()).build(), null).table();
            TableInput tableInput = getTableInput(table.getName(), Optional.empty(), getTableParameters(glueTable));
            awsSyncRequest(glueClient::updateTable, UpdateTableRequest.builder().databaseName(schemaName).tableInput(tableInput).build(), null);
            assertThat(getTableVersions(schemaName, table.getName())).hasSize(2);

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);

            // Verify existing old table versions weren't removed
            List<TableVersion> tableVersionsAfterInsert = getTableVersions(schemaName, table.getName());
            assertThat(tableVersionsAfterInsert).hasSize(2).contains(initialVersion);
        }
    }

    private List<TableVersion> getTableVersions(String databaseName, String tableName)
    {
        ImmutableList.Builder<TableVersion> tableVersionBuilder = ImmutableList.builder();
        GetTableVersionsPublisher tableVersionsPaginator = glueClient.getTableVersionsPaginator(
                GetTableVersionsRequest.builder().databaseName(databaseName).tableName(tableName).build());
        awsSyncPaginatedRequest(tableVersionsPaginator,
                versions -> tableVersionBuilder.addAll(versions.tableVersions()),
                new AwsApiCallStats());
        return tableVersionBuilder.build();
    }
}
