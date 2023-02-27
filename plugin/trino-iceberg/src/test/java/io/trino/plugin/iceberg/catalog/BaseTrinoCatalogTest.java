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
package io.trino.plugin.iceberg.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public abstract class BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(BaseTrinoCatalogTest.class);

    protected abstract TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations);

    protected Map<String, Object> defaultNamespaceProperties(String newNamespaceName)
    {
        return ImmutableMap.of();
    }

    @Test
    public void testCreateNamespaceWithLocation()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_namespace_with_location_" + randomNameSuffix();
        Map<String, Object> namespaceProperties = new HashMap<>(defaultNamespaceProperties(namespace));
        String namespaceLocation = (String) namespaceProperties.computeIfAbsent(LOCATION_PROPERTY, ignored -> "/a/path/");
        namespaceProperties = ImmutableMap.copyOf(namespaceProperties);
        catalog.createNamespace(SESSION, namespace, namespaceProperties, new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        assertThat(catalog.listNamespaces(SESSION)).contains(namespace);
        assertEquals(catalog.loadNamespaceMetadata(SESSION, namespace), namespaceProperties);
        assertEquals(catalog.defaultTableLocation(SESSION, new SchemaTableName(namespace, "table")), namespaceLocation.replaceAll("/$", "") + "/table");
        catalog.dropNamespace(SESSION, namespace);
        assertThat(catalog.listNamespaces(SESSION)).doesNotContain(namespace);
    }

    @Test
    public void testNonLowercaseNamespace()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace" + randomNameSuffix();
        // Trino schema names are always lowercase (until https://github.com/trinodb/trino/issues/17)
        String schema = namespace.toLowerCase(ENGLISH);

        // Currently this is actually stored in lowercase by all Catalogs
        catalog.createNamespace(SESSION, namespace, Map.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                    .isFalse();
            assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                    .isTrue();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(namespace)
                    .contains(schema);

            // Test with IcebergMetadata, should the ConnectorMetadata implementation behavior depend on that class
            ConnectorMetadata icebergMetadata = new IcebergMetadata(
                    PLANNER_CONTEXT.getTypeManager(),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    connectorIdentity -> {
                        throw new UnsupportedOperationException();
                    },
                    new TableStatisticsWriter(new NodeVersion("test-version")));
            assertThat(icebergMetadata.schemaExists(SESSION, namespace)).as("icebergMetadata.schemaExists(namespace)")
                    .isFalse();
            assertThat(icebergMetadata.schemaExists(SESSION, schema)).as("icebergMetadata.schemaExists(schema)")
                    .isTrue();
            assertThat(icebergMetadata.listSchemaNames(SESSION)).as("icebergMetadata.listSchemaNames")
                    .doesNotContain(namespace)
                    .contains(schema);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_table_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        Map<String, String> tableProperties = Map.of("test_key", "test_value");
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            String tableLocation = arbitraryTableLocation(catalog, SESSION, schemaTableName);
            catalog.newCreateTableTransaction(
                            SESSION,
                            schemaTableName,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            tableLocation,
                            tableProperties)
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty())).contains(schemaTableName);

            Table icebergTable = catalog.loadTable(SESSION, schemaTableName);
            assertEquals(icebergTable.name(), quotedTableName(schemaTableName));
            assertEquals(icebergTable.schema().columns().size(), 1);
            assertEquals(icebergTable.schema().columns().get(0).name(), "col1");
            assertEquals(icebergTable.schema().columns().get(0).type(), Types.LongType.get());
            assertEquals(icebergTable.location(), tableLocation);
            assertEquals(icebergTable.sortOrder().isUnsorted(), true);
            assertThat(icebergTable.properties()).containsAllEntriesOf(tableProperties);

            catalog.dropTable(SESSION, schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).doesNotContain(schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty())).doesNotContain(schemaTableName);
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
    public void testCreateWithSortTable()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_sort_table_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            Schema tableSchema = new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get()),
                    Types.NestedField.of(2, true, "col2", Types.StringType.get()),
                    Types.NestedField.of(3, true, "col3", Types.TimestampType.withZone()),
                    Types.NestedField.of(4, true, "col4", Types.StringType.get()));

            SortOrder sortOrder = SortOrder.builderFor(tableSchema)
                    .asc("col1")
                    .desc("col2", NullOrder.NULLS_FIRST)
                    .desc("col3")
                    .desc(Expressions.year("col3"), NullOrder.NULLS_LAST)
                    .desc(Expressions.month("col3"), NullOrder.NULLS_FIRST)
                    .asc(Expressions.day("col3"), NullOrder.NULLS_FIRST)
                    .asc(Expressions.hour("col3"), NullOrder.NULLS_FIRST)
                    .desc(Expressions.bucket("col2", 10), NullOrder.NULLS_FIRST)
                    .desc(Expressions.truncate("col4", 5), NullOrder.NULLS_FIRST).build();
            String tableLocation = arbitraryTableLocation(catalog, SESSION, schemaTableName);
            catalog.newCreateTableTransaction(
                            SESSION,
                            schemaTableName,
                            tableSchema,
                            PartitionSpec.unpartitioned(),
                            sortOrder,
                            tableLocation,
                            ImmutableMap.of())
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty())).contains(schemaTableName);

            Table icebergTable = catalog.loadTable(SESSION, schemaTableName);
            assertEquals(icebergTable.name(), quotedTableName(schemaTableName));
            assertEquals(icebergTable.schema().columns().size(), 4);
            assertEquals(icebergTable.schema().columns().get(0).name(), "col1");
            assertEquals(icebergTable.schema().columns().get(0).type(), Types.LongType.get());
            assertEquals(icebergTable.schema().columns().get(1).name(), "col2");
            assertEquals(icebergTable.schema().columns().get(1).type(), Types.StringType.get());
            assertEquals(icebergTable.location(), tableLocation);
            assertEquals(icebergTable.schema().columns().get(2).name(), "col3");
            assertEquals(icebergTable.schema().columns().get(2).type(), Types.TimestampType.withZone());
            assertEquals(icebergTable.schema().columns().get(3).name(), "col4");
            assertEquals(icebergTable.schema().columns().get(3).type(), Types.StringType.get());
            assertEquals(icebergTable.location(), tableLocation);
            assertEquals(icebergTable.sortOrder(), sortOrder);

            catalog.dropTable(SESSION, schemaTableName);
        }
        finally {
            try {
                if (!catalog.listTables(SESSION, Optional.of(schemaTableName.getSchemaName())).isEmpty()) {
                    catalog.dropTable(SESSION, schemaTableName);
                }
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (RuntimeException e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_rename_table_" + randomNameSuffix();
        String targetNamespace = "test_rename_table_" + randomNameSuffix();

        String table = "tableName";
        SchemaTableName sourceSchemaTableName = new SchemaTableName(namespace, table);
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createNamespace(SESSION, targetNamespace, defaultNamespaceProperties(targetNamespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.newCreateTableTransaction(
                            SESSION,
                            sourceSchemaTableName,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            arbitraryTableLocation(catalog, SESSION, sourceSchemaTableName),
                            ImmutableMap.of())
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(sourceSchemaTableName);

            // Rename within the same schema
            SchemaTableName targetSchemaTableName = new SchemaTableName(sourceSchemaTableName.getSchemaName(), "newTableName");
            catalog.renameTable(SESSION, sourceSchemaTableName, targetSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).doesNotContain(sourceSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(targetSchemaTableName);

            // Move to a different schema
            sourceSchemaTableName = targetSchemaTableName;
            targetSchemaTableName = new SchemaTableName(targetNamespace, sourceSchemaTableName.getTableName());
            catalog.renameTable(SESSION, sourceSchemaTableName, targetSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).doesNotContain(sourceSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(targetNamespace))).contains(targetSchemaTableName);

            catalog.dropTable(SESSION, targetSchemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
                catalog.dropNamespace(SESSION, targetNamespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespaces: %s, %s", namespace, targetNamespace);
            }
        }
    }

    @Test
    public void testUseUniqueTableLocations()
    {
        TrinoCatalog catalog = createTrinoCatalog(true);
        String namespace = "test_unique_table_locations_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        Map<String, Object> namespaceProperties = new HashMap<>(defaultNamespaceProperties(namespace));
        String namespaceLocation = (String) namespaceProperties.computeIfAbsent(LOCATION_PROPERTY, ignored -> {
            try {
                Path tmpDirectory = Files.createTempDirectory("iceberg_catalog_test_rename_table_");
                tmpDirectory.toFile().deleteOnExit();
                return tmpDirectory.toString();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        catalog.createNamespace(SESSION, namespace, namespaceProperties, new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            String location1 = catalog.defaultTableLocation(SESSION, schemaTableName);
            String location2 = catalog.defaultTableLocation(SESSION, schemaTableName);
            assertNotEquals(location1, location2);

            assertThat(location1)
                    .startsWith(namespaceLocation + "/");
            assertThat(location2)
                    .startsWith(namespaceLocation + "/");
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
    public void testView()
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        Path tmpDirectory = Files.createTempDirectory("iceberg_catalog_test_create_view_");
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
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25).getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false);

        try {
            catalog.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createView(SESSION, schemaTableName, viewDefinition, false);

            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(schemaTableName);
            assertThat(catalog.listViews(SESSION, Optional.of(namespace))).contains(schemaTableName);

            Map<SchemaTableName, ConnectorViewDefinition> views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertEquals(views.size(), 1);
            assertViewDefinition(views.get(schemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, schemaTableName).orElseThrow(), viewDefinition);

            catalog.renameView(SESSION, schemaTableName, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).doesNotContain(schemaTableName);
            assertThat(catalog.listViews(SESSION, Optional.of(namespace))).doesNotContain(schemaTableName);
            views = catalog.getViews(SESSION, Optional.of(schemaTableName.getSchemaName()));
            assertEquals(views.size(), 1);
            assertViewDefinition(views.get(renamedSchemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION, renamedSchemaTableName).orElseThrow(), viewDefinition);
            assertThat(catalog.getView(SESSION, schemaTableName)).isEmpty();

            catalog.dropView(SESSION, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.of(namespace)))
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

    private String arbitraryTableLocation(TrinoCatalog catalog, ConnectorSession session, SchemaTableName schemaTableName)
            throws Exception
    {
        try {
            return catalog.defaultTableLocation(session, schemaTableName);
        }
        catch (TrinoException e) {
            if (!e.getErrorCode().equals(HIVE_DATABASE_LOCATION_ERROR.toErrorCode())) {
                throw e;
            }
        }
        Path tmpDirectory = Files.createTempDirectory("iceberg_catalog_test_arbitrary_location");
        tmpDirectory.toFile().deleteOnExit();
        return tmpDirectory.toString();
    }

    private void assertViewDefinition(ConnectorViewDefinition actualView, ConnectorViewDefinition expectedView)
    {
        assertEquals(actualView.getOriginalSql(), expectedView.getOriginalSql());
        assertEquals(actualView.getCatalog(), expectedView.getCatalog());
        assertEquals(actualView.getSchema(), expectedView.getSchema());
        assertEquals(actualView.getColumns().size(), expectedView.getColumns().size());
        for (int i = 0; i < actualView.getColumns().size(); i++) {
            assertViewColumnDefinition(actualView.getColumns().get(i), expectedView.getColumns().get(i));
        }
        assertEquals(actualView.getOwner(), expectedView.getOwner());
        assertEquals(actualView.isRunAsInvoker(), expectedView.isRunAsInvoker());
    }

    private void assertViewColumnDefinition(ConnectorViewDefinition.ViewColumn actualViewColumn, ConnectorViewDefinition.ViewColumn expectedViewColumn)
    {
        assertEquals(actualViewColumn.getName(), expectedViewColumn.getName());
        assertEquals(actualViewColumn.getType(), expectedViewColumn.getType());
    }
}
