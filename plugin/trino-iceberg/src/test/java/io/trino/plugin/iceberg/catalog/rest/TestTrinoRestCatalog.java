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
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.metastore.TableInfo.ExtendedRelationType.OTHER_VIEW;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.NONE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoRestCatalog
        extends BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(TestTrinoRestCatalog.class);

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        return createTrinoRestCatalog(useUniqueTableLocations, ImmutableMap.of());
    }

    private static TrinoRestCatalog createTrinoRestCatalog(boolean useUniqueTableLocations, Map<String, String> properties)
    {
        File warehouseLocation = Files.newTemporaryFolder();
        warehouseLocation.deleteOnExit();

        String catalogName = "iceberg_rest";
        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog
                .builder()
                .delegate(backendCatalog(warehouseLocation))
                .build();

        restSessionCatalog.initialize(catalogName, properties);

        return new TrinoRestCatalog(restSessionCatalog, new CatalogName(catalogName), NONE, ImmutableMap.of(), "test", new TestingTypeManager(), useUniqueTableLocations);
    }

    @Test
    @Override
    public void testNonLowercaseNamespace()
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
                    CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    (connectorIdentity, fileIoProperties) -> {
                        throw new UnsupportedOperationException();
                    },
                    new TableStatisticsWriter(new NodeVersion("test-version")));
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
    @Override
    public void testView()
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        Path tmpDirectory = java.nio.file.Files.createTempDirectory("iceberg_catalog_test_create_view_");
        tmpDirectory.toFile().deleteOnExit();

        String namespace = "test_create_view_" + randomNameSuffix();
        String viewName = "viewName";
        String renamedViewName = "renamedViewName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, viewName);
        SchemaTableName renamedSchemaTableName = new SchemaTableName(namespace, renamedViewName);
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
            catalog.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createView(SESSION, schemaTableName, viewDefinition, false);

            assertThat(catalog.listTables(SESSION, Optional.of(namespace)).stream()).contains(new TableInfo(schemaTableName, OTHER_VIEW));

            Map<SchemaTableName, ConnectorViewDefinition> views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views.size()).isEqualTo(1);
            assertViewDefinition(views.get(schemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, schemaTableName).orElseThrow(), viewDefinition);

            catalog.renameView(SESSION, schemaTableName, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace)).stream().map(TableInfo::tableName).toList()).doesNotContain(schemaTableName);
            views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views.size()).isEqualTo(1);
            assertViewDefinition(views.get(renamedSchemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, renamedSchemaTableName).orElseThrow(), viewDefinition);
            assertThat(catalog.getView(SESSION, schemaTableName)).isEmpty();

            catalog.dropView(SESSION, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty()).stream().map(TableInfo::tableName).toList())
                    .doesNotContain(renamedSchemaTableName);
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

    @Test
    public void testPrefix()
    {
        TrinoCatalog catalog = createTrinoRestCatalog(false, ImmutableMap.of("prefix", "dev"));

        String namespace = "testPrefixNamespace" + randomNameSuffix();

        assertThatThrownBy(() ->
                catalog.createNamespace(
                        SESSION,
                        namespace,
                        defaultNamespaceProperties(namespace),
                        new TrinoPrincipal(PrincipalType.USER, SESSION.getUser())))
                .isInstanceOf(BadRequestException.class)
                .as("should fail as the prefix dev is not implemented for the current endpoint")
                .hasMessageContaining("Malformed request: No route for request: POST v1/dev/namespaces");
    }
}
