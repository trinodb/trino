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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.Duration.ZERO;
import static io.trino.metastore.TableInfo.ExtendedRelationType.OTHER_VIEW;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.iceberg.IcebergTestUtils.TABLE_STATISTICS_READER;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.NONE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoRestCatalog
        extends BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(TestTrinoRestCatalog.class);

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
            throws IOException
    {
        return createTrinoRestCatalog(useUniqueTableLocations, ImmutableMap.of(), Files.createTempDirectory(null));
    }

    @Override
    protected void createNamespaceWithProperties(TrinoCatalog catalog, String namespace, Map<String, String> properties)
    {
        catalog.createNamespace(
                SESSION,
                namespace,
                properties.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
                new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
    }

    private static TrinoRestCatalog createTrinoRestCatalog(boolean useUniqueTableLocations, Map<String, String> properties, Path warehouseLocation)
            throws IOException
    {
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog
                .builder()
                .delegate(backendCatalog(warehouseLocation))
                .build();

        restSessionCatalog.initialize(catalogName, properties);

        return new TrinoRestCatalog(
                new DefaultIcebergFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)),
                restSessionCatalog,
                new CatalogName(catalogName),
                Security.NONE,
                NONE,
                ImmutableMap.of(),
                false,
                "test",
                TESTING_TYPE_MANAGER,
                useUniqueTableLocations,
                false,
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                true);
    }

    @Test
    @Override
    public void testNonLowercaseNamespace()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace" + randomNameSuffix();
        String schema = namespace.toLowerCase(ENGLISH);

        catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                    .isTrue();
            assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                    .isFalse();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(schema)
                    .contains(namespace);

            // Test with IcebergMetadata, should the ConnectorMetadata implementation behavior depend on that class
            ConnectorMetadata icebergMetadata = new IcebergMetadata(
                    PLANNER_CONTEXT.getTypeManager(),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    (connectorIdentity, fileIoProperties) -> {
                        throw new UnsupportedOperationException();
                    },
                    TABLE_STATISTICS_READER,
                    new TableStatisticsWriter(new NodeVersion("test-version")),
                    UNSUPPORTED_DELETION_VECTOR_WRITER,
                    Optional.empty(),
                    false,
                    _ -> false,
                    newDirectExecutorService(),
                    directExecutor(),
                    newDirectExecutorService(),
                    newDirectExecutorService(),
                    0,
                    ZERO);
            assertThat(icebergMetadata.schemaExists(SESSION, namespace)).as("icebergMetadata.schemaExists(namespace)")
                    .isTrue();
            assertThat(icebergMetadata.schemaExists(SESSION, schema)).as("icebergMetadata.schemaExists(schema)")
                    .isFalse();
            assertThat(icebergMetadata.listSchemaNames(SESSION)).as("icebergMetadata.listSchemaNames")
                    .doesNotContain(schema)
                    .contains(namespace);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }

    @Test
    public void testPrefix()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoRestCatalog(false, ImmutableMap.of("prefix", "dev"), Files.createTempDirectory(null));

        String namespace = "testPrefixNamespace" + randomNameSuffix();

        assertThatThrownBy(() ->
                catalog.createNamespace(
                        SESSION,
                        namespace,
                        defaultNamespaceProperties(namespace),
                        new TrinoPrincipal(PrincipalType.USER, SESSION.getUser())))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to create namespace")
                .cause()
                .as("should fail as the prefix dev is not implemented for the current endpoint")
                .hasMessageContaining("Malformed request: No route for request: POST v1/dev/namespaces");
    }

    @Override
    protected TableInfo.ExtendedRelationType getViewType()
    {
        return OTHER_VIEW;
    }

    @Test
    public void testCreateReplaceViewUniqueLocation()
            throws IOException
    {
        Path warehouseLocation = Files.createTempDirectory("iceberg_catalog_test_create_view_");
        TrinoRestCatalog catalog = createTrinoRestCatalog(true, ImmutableMap.of(), warehouseLocation);

        String namespace = "test_create_view_" + randomNameSuffix();
        String viewName = "viewName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, viewName);
        ConnectorViewDefinition viewDefinition = new ConnectorViewDefinition(
                "SELECT name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createUnboundedVarcharType().getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false,
                ImmutableList.of());

        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createView(SESSION, schemaTableName, viewDefinition, false);

            assertThat(catalog.listTables(SESSION, Optional.of(namespace)).stream()).contains(new TableInfo(schemaTableName, getViewType()));

            Map<SchemaTableName, ConnectorViewDefinition> views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views).hasSize(1);
            assertViewDefinition(views.get(schemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, schemaTableName).orElseThrow(), viewDefinition);

            View initialViewLocation = catalog.getIcebergView(SESSION, schemaTableName, false).orElse(null);
            assertThat(initialViewLocation).isNotNull();
            assertThat(initialViewLocation.name()).isNotNull();
            assertThat(initialViewLocation.name().toLowerCase(ENGLISH)).endsWith((namespace + "." + viewName).toLowerCase(ENGLISH));
            assertThat(initialViewLocation.location()).isNotNull();

            catalog.createView(SESSION, schemaTableName, viewDefinition, true);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace)).stream()).contains(new TableInfo(schemaTableName, getViewType()));
            views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views).hasSize(1);
            assertViewDefinition(views.get(schemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, schemaTableName).orElseThrow(), viewDefinition);

            View finalViewLocation = catalog.getIcebergView(SESSION, schemaTableName, false).orElse(null);
            assertThat(finalViewLocation).isNotNull();
            assertThat(finalViewLocation.name()).isNotNull();
            assertThat(finalViewLocation.name().toLowerCase(ENGLISH)).endsWith((namespace + "." + viewName).toLowerCase(ENGLISH));
            assertThat(finalViewLocation.location()).isNotNull();
            assertThat(finalViewLocation.location()).isEqualTo(initialViewLocation.location());

            catalog.dropView(SESSION, schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty()).stream().map(TableInfo::tableName).toList())
                    .doesNotContain(schemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }
}
