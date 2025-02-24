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
import io.trino.metastore.TableInfo;
import io.trino.metastore.TableInfo.ExtendedRelationType;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TABLE;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TRINO_VIEW;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(BaseTrinoCatalogTest.class);
    protected static final ConnectorSession SESSION_WITH_PROPERTIES = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    protected abstract TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
            throws IOException;

    protected Map<String, Object> defaultNamespaceProperties(String newNamespaceName)
    {
        return ImmutableMap.of();
    }

    @Test
    public void testCreateNamespaceWithLocation()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_namespace_with_location_" + randomNameSuffix();
        Map<String, Object> namespaceProperties = new HashMap<>(defaultNamespaceProperties(namespace));
        String namespaceLocation = (String) namespaceProperties.computeIfAbsent(LOCATION_PROPERTY, _ -> "local:///a/path/");
        namespaceProperties = ImmutableMap.copyOf(namespaceProperties);
        catalog.createNamespace(SESSION_WITH_PROPERTIES, namespace, namespaceProperties, new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
        assertThat(catalog.listNamespaces(SESSION_WITH_PROPERTIES)).contains(namespace);
        assertThat(catalog.loadNamespaceMetadata(SESSION_WITH_PROPERTIES, namespace)).isEqualTo(namespaceProperties);
        assertThat(catalog.defaultTableLocation(SESSION_WITH_PROPERTIES, new SchemaTableName(namespace, "table"))).isEqualTo(namespaceLocation.replaceAll("/$", "") + "/table");
        catalog.dropNamespace(SESSION_WITH_PROPERTIES, namespace);
        assertThat(catalog.listNamespaces(SESSION_WITH_PROPERTIES)).doesNotContain(namespace);
    }

    @Test
    public void testNonLowercaseNamespace()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace" + randomNameSuffix();
        // Trino schema names are always lowercase (until https://github.com/trinodb/trino/issues/17)
        String schema = namespace.toLowerCase(ENGLISH);

        // Currently this is actually stored in lowercase by all Catalogs
        catalog.createNamespace(SESSION_WITH_PROPERTIES, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION_WITH_PROPERTIES, namespace)).as("catalog.namespaceExists(namespace)")
                    .isFalse();
            assertThat(catalog.namespaceExists(SESSION_WITH_PROPERTIES, schema)).as("catalog.namespaceExists(schema)")
                    .isTrue();
            assertThat(catalog.listNamespaces(SESSION_WITH_PROPERTIES)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(namespace)
                    .contains(schema);

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
            assertThat(icebergMetadata.schemaExists(SESSION_WITH_PROPERTIES, namespace)).as("icebergMetadata.schemaExists(namespace)")
                    .isFalse();
            assertThat(icebergMetadata.schemaExists(SESSION_WITH_PROPERTIES, schema)).as("icebergMetadata.schemaExists(schema)")
                    .isTrue();
            assertThat(icebergMetadata.listSchemaNames(SESSION_WITH_PROPERTIES)).as("icebergMetadata.listSchemaNames")
                    .doesNotContain(namespace)
                    .contains(schema);
        }
        finally {
            catalog.dropNamespace(SESSION_WITH_PROPERTIES, namespace);
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
            catalog.createNamespace(SESSION_WITH_PROPERTIES, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
            String tableLocation = arbitraryTableLocation(catalog, SESSION_WITH_PROPERTIES, schemaTableName);
            catalog.newCreateTableTransaction(
                            SESSION_WITH_PROPERTIES,
                            schemaTableName,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            Optional.of(tableLocation),
                            tableProperties)
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(namespace))).contains(new TableInfo(schemaTableName, TABLE));
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.empty())).contains(new TableInfo(schemaTableName, TABLE));

            Table icebergTable = catalog.loadTable(SESSION_WITH_PROPERTIES, schemaTableName);
            assertThat(icebergTable.name()).isEqualTo(quotedTableName(schemaTableName));
            assertThat(icebergTable.schema().columns()).hasSize(1);
            assertThat(icebergTable.schema().columns().get(0).name()).isEqualTo("col1");
            assertThat(icebergTable.schema().columns().get(0).type()).isEqualTo(Types.LongType.get());
            assertThat(icebergTable.location()).isEqualTo(tableLocation);
            assertThat(icebergTable.sortOrder().isUnsorted()).isEqualTo(true);
            assertThat(icebergTable.properties()).containsAllEntriesOf(tableProperties);

            catalog.dropTable(SESSION_WITH_PROPERTIES, schemaTableName);
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.empty()).stream().map(TableInfo::tableName).toList()).doesNotContain(schemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION_WITH_PROPERTIES, namespace);
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
            catalog.createNamespace(SESSION_WITH_PROPERTIES, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
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
            String tableLocation = arbitraryTableLocation(catalog, SESSION_WITH_PROPERTIES, schemaTableName);
            catalog.newCreateTableTransaction(
                            SESSION_WITH_PROPERTIES,
                            schemaTableName,
                            tableSchema,
                            PartitionSpec.unpartitioned(),
                            sortOrder,
                            Optional.of(tableLocation),
                            ImmutableMap.of())
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(namespace))).contains(new TableInfo(schemaTableName, TABLE));
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.empty())).contains(new TableInfo(schemaTableName, TABLE));

            Table icebergTable = catalog.loadTable(SESSION_WITH_PROPERTIES, schemaTableName);
            assertThat(icebergTable.name()).isEqualTo(quotedTableName(schemaTableName));
            assertThat(icebergTable.schema().columns()).hasSize(4);
            assertThat(icebergTable.schema().columns().get(0).name()).isEqualTo("col1");
            assertThat(icebergTable.schema().columns().get(0).type()).isEqualTo(Types.LongType.get());
            assertThat(icebergTable.schema().columns().get(1).name()).isEqualTo("col2");
            assertThat(icebergTable.schema().columns().get(1).type()).isEqualTo(Types.StringType.get());
            assertThat(icebergTable.location()).isEqualTo(tableLocation);
            assertThat(icebergTable.schema().columns().get(2).name()).isEqualTo("col3");
            assertThat(icebergTable.schema().columns().get(2).type()).isEqualTo(Types.TimestampType.withZone());
            assertThat(icebergTable.schema().columns().get(3).name()).isEqualTo("col4");
            assertThat(icebergTable.schema().columns().get(3).type()).isEqualTo(Types.StringType.get());
            assertThat(icebergTable.location()).isEqualTo(tableLocation);
            assertThat(icebergTable.sortOrder()).isEqualTo(sortOrder);

            catalog.dropTable(SESSION_WITH_PROPERTIES, schemaTableName);
        }
        finally {
            try {
                if (!catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(schemaTableName.getSchemaName())).isEmpty()) {
                    catalog.dropTable(SESSION_WITH_PROPERTIES, schemaTableName);
                }
                catalog.dropNamespace(SESSION_WITH_PROPERTIES, namespace);
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
            catalog.createNamespace(SESSION_WITH_PROPERTIES, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
            catalog.createNamespace(SESSION_WITH_PROPERTIES, targetNamespace, defaultNamespaceProperties(targetNamespace), new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
            catalog.newCreateTableTransaction(
                            SESSION_WITH_PROPERTIES,
                            sourceSchemaTableName,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            Optional.of(arbitraryTableLocation(catalog, SESSION_WITH_PROPERTIES, sourceSchemaTableName)),
                            ImmutableMap.of())
                    .commitTransaction();
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(namespace))).contains(new TableInfo(sourceSchemaTableName, TABLE));

            // Rename within the same schema
            SchemaTableName targetSchemaTableName = new SchemaTableName(sourceSchemaTableName.getSchemaName(), "newTableName");
            catalog.renameTable(SESSION_WITH_PROPERTIES, sourceSchemaTableName, targetSchemaTableName);
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.empty()).stream().map(TableInfo::tableName).toList()).doesNotContain(sourceSchemaTableName);
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(namespace))).contains(new TableInfo(targetSchemaTableName, TABLE));

            // Move to a different schema
            sourceSchemaTableName = targetSchemaTableName;
            targetSchemaTableName = new SchemaTableName(targetNamespace, sourceSchemaTableName.getTableName());
            catalog.renameTable(SESSION_WITH_PROPERTIES, sourceSchemaTableName, targetSchemaTableName);
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(namespace)).stream().map(TableInfo::tableName).toList()).doesNotContain(sourceSchemaTableName);
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(targetNamespace))).contains(new TableInfo(targetSchemaTableName, TABLE));

            catalog.dropTable(SESSION_WITH_PROPERTIES, targetSchemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION_WITH_PROPERTIES, namespace);
                catalog.dropNamespace(SESSION_WITH_PROPERTIES, targetNamespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespaces: %s, %s", namespace, targetNamespace);
            }
        }
    }

    @Test
    public void testUseUniqueTableLocations()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(true);
        String namespace = "test_unique_table_locations_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        Map<String, Object> namespaceProperties = new HashMap<>(defaultNamespaceProperties(namespace));
        String namespaceLocation = (String) namespaceProperties.computeIfAbsent(
                LOCATION_PROPERTY,
                _ -> "local:///iceberg_catalog_test_rename_table_" + UUID.randomUUID());

        catalog.createNamespace(SESSION_WITH_PROPERTIES, namespace, namespaceProperties, new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
        try {
            String location1 = catalog.defaultTableLocation(SESSION_WITH_PROPERTIES, schemaTableName);
            String location2 = catalog.defaultTableLocation(SESSION_WITH_PROPERTIES, schemaTableName);
            assertThat(location1)
                    .isNotEqualTo(location2);

            assertThat(location1)
                    .startsWith(namespaceLocation + "/");
            assertThat(location2)
                    .startsWith(namespaceLocation + "/");
        }
        finally {
            try {
                catalog.dropNamespace(SESSION_WITH_PROPERTIES, namespace);
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
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createUnboundedVarcharType().getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION_WITH_PROPERTIES.getUser()),
                false,
                ImmutableList.of());

        try {
            catalog.createNamespace(SESSION_WITH_PROPERTIES, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser()));
            catalog.createView(SESSION_WITH_PROPERTIES, schemaTableName, viewDefinition, false);

            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(namespace)).stream()).contains(new TableInfo(schemaTableName, getViewType()));

            Map<SchemaTableName, ConnectorViewDefinition> views = catalog.getViews(SESSION_WITH_PROPERTIES, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views).hasSize(1);
            assertViewDefinition(views.get(schemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION_WITH_PROPERTIES, schemaTableName).orElseThrow(), viewDefinition);

            catalog.renameView(SESSION_WITH_PROPERTIES, schemaTableName, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(namespace)).stream().map(TableInfo::tableName).toList()).doesNotContain(schemaTableName);
            views = catalog.getViews(SESSION_WITH_PROPERTIES, Optional.of(schemaTableName.getSchemaName()));
            assertThat(views).hasSize(1);
            assertViewDefinition(views.get(renamedSchemaTableName), viewDefinition);
            assertViewDefinition(catalog.getView(SESSION_WITH_PROPERTIES, renamedSchemaTableName).orElseThrow(), viewDefinition);
            assertThat(catalog.getView(SESSION_WITH_PROPERTIES, schemaTableName)).isEmpty();

            catalog.dropView(SESSION_WITH_PROPERTIES, renamedSchemaTableName);
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.empty()).stream().map(TableInfo::tableName).toList())
                    .doesNotContain(renamedSchemaTableName);
        }
        finally {
            try {
                catalog.dropNamespace(SESSION_WITH_PROPERTIES, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    protected ExtendedRelationType getViewType()
    {
        return TRINO_VIEW;
    }

    @Test
    public void testListTables()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        TrinoPrincipal principal = new TrinoPrincipal(PrincipalType.USER, SESSION_WITH_PROPERTIES.getUser());

        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            String ns1 = "ns1" + randomNameSuffix();
            String ns2 = "ns2" + randomNameSuffix();
            catalog.createNamespace(SESSION_WITH_PROPERTIES, ns1, defaultNamespaceProperties(ns1), principal);
            closer.register(() -> catalog.dropNamespace(SESSION_WITH_PROPERTIES, ns1));
            catalog.createNamespace(SESSION_WITH_PROPERTIES, ns2, defaultNamespaceProperties(ns2), principal);
            closer.register(() -> catalog.dropNamespace(SESSION_WITH_PROPERTIES, ns2));

            SchemaTableName table1 = new SchemaTableName(ns1, "t1");
            SchemaTableName table2 = new SchemaTableName(ns2, "t2");
            catalog.newCreateTableTransaction(
                            SESSION_WITH_PROPERTIES,
                            table1,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            Optional.of(arbitraryTableLocation(catalog, SESSION_WITH_PROPERTIES, table1)),
                            ImmutableMap.of())
                    .commitTransaction();
            closer.register(() -> catalog.dropTable(SESSION_WITH_PROPERTIES, table1));

            catalog.newCreateTableTransaction(
                            SESSION_WITH_PROPERTIES,
                            table2,
                            new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                            PartitionSpec.unpartitioned(),
                            SortOrder.unsorted(),
                            Optional.of(arbitraryTableLocation(catalog, SESSION_WITH_PROPERTIES, table2)),
                            ImmutableMap.of())
                    .commitTransaction();
            closer.register(() -> catalog.dropTable(SESSION_WITH_PROPERTIES, table2));

            ImmutableList.Builder<TableInfo> allTables = ImmutableList.<TableInfo>builder()
                    .add(new TableInfo(table1, TABLE))
                    .add(new TableInfo(table2, TABLE));

            ImmutableList.Builder<SchemaTableName> icebergTables = ImmutableList.<SchemaTableName>builder()
                    .add(table1)
                    .add(table2);
            SchemaTableName view = new SchemaTableName(ns2, "view");
            try {
                catalog.createView(
                        SESSION_WITH_PROPERTIES,
                        view,
                        new ConnectorViewDefinition(
                                "SELECT name FROM local.tiny.nation",
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(
                                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createUnboundedVarcharType().getTypeId(), Optional.empty())),
                                Optional.empty(),
                                Optional.of(SESSION_WITH_PROPERTIES.getUser()),
                                false,
                                ImmutableList.of()),
                        false);
                closer.register(() -> catalog.dropView(SESSION_WITH_PROPERTIES, view));
                allTables.add(new TableInfo(view, getViewType()));
            }
            catch (TrinoException e) {
                assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
            }

            try {
                SchemaTableName materializedView = new SchemaTableName(ns2, "mv");
                createMaterializedView(
                        SESSION_WITH_PROPERTIES,
                        catalog,
                        materializedView,
                        someMaterializedView(),
                        ImmutableMap.of(
                                FILE_FORMAT_PROPERTY, IcebergFileFormat.PARQUET,
                                FORMAT_VERSION_PROPERTY, 1),
                        false,
                        false);
                closer.register(() -> catalog.dropMaterializedView(SESSION_WITH_PROPERTIES, materializedView));
                allTables.add(new TableInfo(materializedView, TRINO_MATERIALIZED_VIEW));
            }
            catch (TrinoException e) {
                assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
            }

            createExternalIcebergTable(catalog, ns2, closer).ifPresent(table -> {
                allTables.add(new TableInfo(table, TABLE));
                icebergTables.add(table);
            });
            createExternalNonIcebergTable(catalog, ns2, closer).ifPresent(table -> {
                allTables.add(new TableInfo(table, TABLE));
            });

            // No namespace provided, all tables across all namespaces should be returned
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.empty())).containsAll(allTables.build());
            assertThat(catalog.listIcebergTables(SESSION_WITH_PROPERTIES, Optional.empty())).containsAll(icebergTables.build());
            // Namespace is provided and exists
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of(ns1))).containsExactly(new TableInfo(table1, TABLE));
            assertThat(catalog.listIcebergTables(SESSION_WITH_PROPERTIES, Optional.of(ns1))).containsExactly(table1);
            // Namespace is provided and does not exist
            assertThat(catalog.listTables(SESSION_WITH_PROPERTIES, Optional.of("non_existing"))).isEmpty();
            assertThat(catalog.listIcebergTables(SESSION_WITH_PROPERTIES, Optional.of("non_existing"))).isEmpty();
        }
    }

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
                properties,
                replace,
                ignoreExisting);
    }

    protected Optional<SchemaTableName> createExternalIcebergTable(TrinoCatalog catalog, String namespace, AutoCloseableCloser closer)
            throws Exception
    {
        return Optional.empty();
    }

    protected Optional<SchemaTableName> createExternalNonIcebergTable(TrinoCatalog catalog, String namespace, AutoCloseableCloser closer)
            throws Exception
    {
        return Optional.empty();
    }

    protected void assertViewDefinition(ConnectorViewDefinition actualView, ConnectorViewDefinition expectedView)
    {
        assertThat(actualView.getOriginalSql()).isEqualTo(expectedView.getOriginalSql());
        assertThat(actualView.getCatalog()).isEqualTo(expectedView.getCatalog());
        assertThat(actualView.getSchema()).isEqualTo(expectedView.getSchema());
        assertThat(actualView.getColumns()).hasSize(expectedView.getColumns().size());
        for (int i = 0; i < actualView.getColumns().size(); i++) {
            assertViewColumnDefinition(actualView.getColumns().get(i), expectedView.getColumns().get(i));
        }
        assertThat(actualView.getOwner()).isEqualTo(expectedView.getOwner());
        assertThat(actualView.isRunAsInvoker()).isEqualTo(expectedView.isRunAsInvoker());
    }

    protected String arbitraryTableLocation(TrinoCatalog catalog, ConnectorSession session, SchemaTableName schemaTableName)
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

    private void assertViewColumnDefinition(ConnectorViewDefinition.ViewColumn actualViewColumn, ConnectorViewDefinition.ViewColumn expectedViewColumn)
    {
        assertThat(actualViewColumn.getName()).isEqualTo(expectedViewColumn.getName());
        assertThat(actualViewColumn.getType()).isEqualTo(expectedViewColumn.getType());
    }

    private static ConnectorMaterializedViewDefinition someMaterializedView()
    {
        return new ConnectorMaterializedViewDefinition(
                "select 1",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("test", BIGINT.getTypeId(), Optional.empty())),
                Optional.of(Duration.ZERO),
                Optional.empty(),
                Optional.of("owner"),
                ImmutableList.of());
    }
}
