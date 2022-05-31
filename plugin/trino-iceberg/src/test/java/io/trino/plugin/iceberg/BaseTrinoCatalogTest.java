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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotEquals;

public abstract class BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(BaseTrinoCatalogTest.class);

    protected abstract TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations);

    @Test
    public void testCreateNamespaceWithLocation()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "test_create_namespace_with_location_" + randomTableSuffix();
        catalog.createNamespace(SESSION, namespace, ImmutableMap.of(LOCATION_PROPERTY, "/a/path/"), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        assertThat(catalog.listNamespaces(SESSION)).contains(namespace);
        assertEquals(catalog.loadNamespaceMetadata(SESSION, namespace), ImmutableMap.of(LOCATION_PROPERTY, "/a/path/"));
        assertEquals(catalog.defaultTableLocation(SESSION, new SchemaTableName(namespace, "table")), "/a/path/table");
        catalog.dropNamespace(SESSION, namespace);
        assertThat(catalog.listNamespaces(SESSION)).doesNotContain(namespace);
    }

    @Test
    public void testCreateTable()
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        Path tmpDirectory = Files.createTempDirectory("iceberg_catalog_test_create_table_");
        tmpDirectory.toFile().deleteOnExit();

        String namespace = "test_create_table_" + randomTableSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        try {
            catalog.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.newCreateTableTransaction(
                    SESSION,
                    schemaTableName,
                    new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                    PartitionSpec.unpartitioned(),
                    tmpDirectory.toAbsolutePath().toString(),
                    ImmutableMap.of())
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION, Optional.of(namespace))).contains(schemaTableName);
            assertThat(catalog.listTables(SESSION, Optional.empty())).contains(schemaTableName);

            Table icebergTable = catalog.loadTable(SESSION, schemaTableName);
            assertEquals(icebergTable.name(), quotedTableName(schemaTableName));
            assertEquals(icebergTable.schema().columns().size(), 1);
            assertEquals(icebergTable.schema().columns().get(0).name(), "col1");
            assertEquals(icebergTable.schema().columns().get(0).type(), Types.LongType.get());
            assertEquals(icebergTable.location(), tmpDirectory.toAbsolutePath().toString());
            assertEquals(icebergTable.properties(), ImmutableMap.of());

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
    public void testRenameTable()
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        Path tmpDirectory = Files.createTempDirectory("iceberg_catalog_test_rename_table_");
        tmpDirectory.toFile().deleteOnExit();

        String namespace = "test_rename_table_" + randomTableSuffix();
        String targetNamespace = "test_rename_table_" + randomTableSuffix();

        String table = "tableName";
        SchemaTableName sourceSchemaTableName = new SchemaTableName(namespace, table);
        try {
            catalog.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createNamespace(SESSION, targetNamespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.newCreateTableTransaction(
                    SESSION,
                    sourceSchemaTableName,
                    new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                    PartitionSpec.unpartitioned(),
                    tmpDirectory.toAbsolutePath().toString(),
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
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog(true);
        Path tmpDirectory = Files.createTempDirectory("iceberg_catalog_test_rename_table_");
        tmpDirectory.toFile().deleteOnExit();

        String namespace = "test_unique_table_locations_" + randomTableSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        catalog.createNamespace(SESSION, namespace, ImmutableMap.of(LOCATION_PROPERTY, tmpDirectory.toString()), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            String location1 = catalog.defaultTableLocation(SESSION, schemaTableName);
            String location2 = catalog.defaultTableLocation(SESSION, schemaTableName);
            assertNotEquals(location1, location2);

            assertEquals(Path.of(location1).getParent(), tmpDirectory);
            assertEquals(Path.of(location2).getParent(), tmpDirectory);
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

        String namespace = "test_create_view_" + randomTableSuffix();
        String viewName = "viewName";
        String renamedViewName = "renamedViewName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, viewName);
        SchemaTableName renamedSchemaTableName = new SchemaTableName(namespace, renamedViewName);
        ConnectorViewDefinition viewDefinition = new ConnectorViewDefinition(
                "SELECT name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25).getTypeId())),
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
                LOG.warn("Failed to clean up namespace: " + namespace);
            }
        }
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
