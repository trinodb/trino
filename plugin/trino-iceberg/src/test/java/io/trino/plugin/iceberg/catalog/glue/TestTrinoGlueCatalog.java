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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoGlueCatalog
        extends BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(TestTrinoGlueCatalog.class);

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        return createGlueTrinoCatalog(useUniqueTableLocations, false);
    }

    private TrinoCatalog createGlueTrinoCatalog(boolean useUniqueTableLocations, boolean useSystemSecurity)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        IcebergGlueCatalogConfig catalogConfig = new IcebergGlueCatalogConfig();
        return new TrinoGlueCatalog(
                new CatalogName("catalog_name"),
                HDFS_FILE_SYSTEM_FACTORY,
                new TestingTypeManager(),
                catalogConfig.isCacheTableMetadata(),
                new GlueIcebergTableOperationsProvider(
                        TESTING_TYPE_MANAGER,
                        catalogConfig,
                        HDFS_FILE_SYSTEM_FACTORY,
                        new GlueMetastoreStats(),
                        glueClient),
                "test",
                glueClient,
                new GlueMetastoreStats(),
                useSystemSecurity,
                Optional.empty(),
                useUniqueTableLocations,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                directExecutor());
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
                    CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    (connectorIdentity, fileIoProperties) -> {
                        throw new UnsupportedOperationException();
                    },
                    new TableStatisticsWriter(new NodeVersion("test-version")),
                    Optional.empty(),
                    false,
                    _ -> false,
                    newDirectExecutorService(),
                    directExecutor());
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
    public void testCreateMaterializedViewWithSystemSecurity()
    {
        TrinoCatalog glueTrinoCatalog = createGlueTrinoCatalog(false, true);
        String namespace = "test_create_mv_" + randomNameSuffix();
        String table = "materialized_view_name";
        SchemaTableName viewName = new SchemaTableName(namespace, table);
        Map<String, Object> properties = ImmutableMap.of(LOCATION_PROPERTY, "file:///tmp/a/path/");
        try {
            glueTrinoCatalog.createNamespace(SESSION, namespace, properties, new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            glueTrinoCatalog.createMaterializedView(
                    SESSION,
                    viewName,
                    new ConnectorMaterializedViewDefinition(
                            "CREATE * FROM tpch.tiny.nations",
                            Optional.empty(),
                            Optional.of("catalog_name"),
                            Optional.of("schema_name"),
                            ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("col1", INTEGER.getTypeId(), Optional.empty())),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of("test_owner"),
                            ImmutableList.of()),
                    ImmutableMap.of(FILE_FORMAT_PROPERTY, PARQUET, FORMAT_VERSION_PROPERTY, 1),
                    false,
                    false);
            List<SchemaTableName> materializedViews = glueTrinoCatalog.listTables(SESSION, Optional.of(namespace)).stream()
                    .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW)
                    .map(TableInfo::tableName)
                    .toList();
            assertThat(materializedViews).hasSize(1);
            assertThat(materializedViews.get(0).getTableName()).isEqualTo(table);
            Optional<ConnectorMaterializedViewDefinition> returned = glueTrinoCatalog.getMaterializedView(SESSION, materializedViews.get(0));
            assertThat(returned).isPresent();
            assertThat(returned.get().getOwner()).isEmpty();
        }
        finally {
            try {
                glueTrinoCatalog.dropMaterializedView(SESSION, viewName);
            }
            catch (MaterializedViewNotFoundException e) {
                LOG.warn("Failed to clean up view: %s", viewName);
            }
            try {
                glueTrinoCatalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
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
        IcebergGlueCatalogConfig catalogConfig = new IcebergGlueCatalogConfig();
        TrinoCatalog catalogWithDefaultLocation = new TrinoGlueCatalog(
                new CatalogName("catalog_name"),
                fileSystemFactory,
                new TestingTypeManager(),
                catalogConfig.isCacheTableMetadata(),
                new GlueIcebergTableOperationsProvider(
                        TESTING_TYPE_MANAGER,
                        catalogConfig,
                        fileSystemFactory,
                        new GlueMetastoreStats(),
                        glueClient),
                "test",
                glueClient,
                new GlueMetastoreStats(),
                false,
                Optional.of(tmpDirectory.toAbsolutePath().toString()),
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                directExecutor());

        String namespace = "test_default_location_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        catalogWithDefaultLocation.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            File expectedSchemaDirectory = new File(tmpDirectory.toFile(), namespace + ".db");
            File expectedTableDirectory = new File(expectedSchemaDirectory, schemaTableName.getTableName());
            assertThat(catalogWithDefaultLocation.defaultTableLocation(SESSION, schemaTableName)).isEqualTo(expectedTableDirectory.toPath().toAbsolutePath().toString());
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

    @Override
    protected void createMaterializedView(
            ConnectorSession session,
            TrinoCatalog catalog,
            SchemaTableName materializedView,
            ConnectorMaterializedViewDefinition materializedViewDefinition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        catalog.createMaterializedView(
                session,
                materializedView,
                materializedViewDefinition,
                ImmutableMap.<String, Object>builder()
                        .putAll(properties)
                        .put(LOCATION_PROPERTY, "file:///tmp/a/path/" + materializedView.getTableName())
                        .buildOrThrow(),
                replace,
                ignoreExisting);
    }
}
