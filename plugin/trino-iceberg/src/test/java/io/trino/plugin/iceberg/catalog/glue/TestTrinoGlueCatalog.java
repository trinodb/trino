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
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestTrinoGlueCatalog
        extends BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(TestTrinoGlueCatalog.class);

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        TrinoFileSystemFactory fileSystemFactory = HDFS_FILE_SYSTEM_FACTORY;
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        return new TrinoGlueCatalog(
                new CatalogName("catalog_name"),
                fileSystemFactory,
                new TestingTypeManager(),
                new GlueIcebergTableOperationsProvider(
                        fileSystemFactory,
                        new GlueMetastoreStats(),
                        glueClient),
                "test",
                glueClient,
                new GlueMetastoreStats(),
                Optional.empty(),
                useUniqueTableLocations);
    }

    /**
     * Similar to {@link #testNonLowercaseNamespace()}, but creates the Glue database via Glue API, in case Glue starts allowing non-lowercase names.
     */
    @Test
    public void testNonLowercaseGlueDatabase()
    {
        String databaseName = "testNonLowercaseDatabase" + randomNameSuffix();
        // Trino schema names are always lowercase (until https://github.com/trinodb/trino/issues/17)
        String trinoSchemaName = databaseName.toLowerCase(ENGLISH);

        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        glueClient.createDatabase(new CreateDatabaseRequest()
                .withDatabaseInput(new DatabaseInput()
                        // Currently this is actually stored in lowercase
                        .withName(databaseName)));
        try {
            TrinoCatalog catalog = createTrinoCatalog(false);
            assertThat(catalog.namespaceExists(SESSION, databaseName)).as("catalog.namespaceExists(databaseName)")
                    .isFalse();
            assertThat(catalog.namespaceExists(SESSION, trinoSchemaName)).as("catalog.namespaceExists(trinoSchemaName)")
                    .isTrue();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(databaseName)
                    .contains(trinoSchemaName);

            // Test with IcebergMetadata, should the ConnectorMetadata implementation behavior depend on that class
            ConnectorMetadata icebergMetadata = new IcebergMetadata(
                    PLANNER_CONTEXT.getTypeManager(),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    connectorIdentity -> {
                        throw new UnsupportedOperationException();
                    },
                    new TableStatisticsWriter(new NodeVersion("test-version")));
            assertThat(icebergMetadata.schemaExists(SESSION, databaseName)).as("icebergMetadata.schemaExists(databaseName)")
                    .isFalse();
            assertThat(icebergMetadata.schemaExists(SESSION, trinoSchemaName)).as("icebergMetadata.schemaExists(trinoSchemaName)")
                    .isTrue();
            assertThat(icebergMetadata.listSchemaNames(SESSION)).as("icebergMetadata.listSchemaNames")
                    .doesNotContain(databaseName)
                    .contains(trinoSchemaName);
        }
        finally {
            glueClient.deleteDatabase(new DeleteDatabaseRequest()
                    .withName(databaseName));
        }
    }

    @Test
    public void testDefaultLocation()
            throws IOException
    {
        Path tmpDirectory = Files.createTempDirectory("test_glue_catalog_default_location_");
        tmpDirectory.toFile().deleteOnExit();

        TrinoFileSystemFactory fileSystemFactory = HDFS_FILE_SYSTEM_FACTORY;
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        TrinoCatalog catalogWithDefaultLocation = new TrinoGlueCatalog(
                new CatalogName("catalog_name"),
                fileSystemFactory,
                new TestingTypeManager(),
                new GlueIcebergTableOperationsProvider(
                        fileSystemFactory,
                        new GlueMetastoreStats(),
                        glueClient),
                "test",
                glueClient,
                new GlueMetastoreStats(),
                Optional.of(tmpDirectory.toAbsolutePath().toString()),
                false);

        String namespace = "test_default_location_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        catalogWithDefaultLocation.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            File expectedSchemaDirectory = new File(tmpDirectory.toFile(), namespace + ".db");
            File expectedTableDirectory = new File(expectedSchemaDirectory, schemaTableName.getTableName());
            assertEquals(catalogWithDefaultLocation.defaultTableLocation(SESSION, schemaTableName), expectedTableDirectory.toPath().toAbsolutePath().toString());
        }
        finally {
            try {
                catalogWithDefaultLocation.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }
}
