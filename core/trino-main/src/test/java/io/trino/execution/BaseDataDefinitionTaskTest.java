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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TableSchema;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.connector.SaveMode.IGNORE;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public abstract class BaseDataDefinitionTaskTest
{
    public static final String SCHEMA = "schema";

    protected static final String COLUMN_PROPERTY_NAME = "column_property";
    protected static final Long COLUMN_PROPERTY_DEFAULT_VALUE = null;

    protected static final String MATERIALIZED_VIEW_PROPERTY_1_NAME = "property1";
    protected static final Long MATERIALIZED_VIEW_PROPERTY_1_DEFAULT_VALUE = null;

    protected static final String MATERIALIZED_VIEW_PROPERTY_2_NAME = "property2";
    protected static final String MATERIALIZED_VIEW_PROPERTY_2_DEFAULT_VALUE = "defaultProperty2Value";
    protected static final Map<String, Object> MATERIALIZED_VIEW_PROPERTIES = ImmutableMap.of(MATERIALIZED_VIEW_PROPERTY_2_NAME, MATERIALIZED_VIEW_PROPERTY_2_DEFAULT_VALUE);

    private QueryRunner queryRunner;
    protected Session testSession;
    protected MockMetadata metadata;
    protected PlannerContext plannerContext;
    protected ColumnPropertyManager columnPropertyManager;
    protected MaterializedViewPropertyManager materializedViewPropertyManager;
    protected TransactionManager transactionManager;
    protected QueryStateMachine queryStateMachine;

    @BeforeEach
    public void setUp()
    {
        testSession = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .build();
        queryRunner = new StandaloneQueryRunner(testSession);
        transactionManager = queryRunner.getTransactionManager();
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.create("initial")));
        queryRunner.createCatalog(TEST_CATALOG_NAME, "initial", ImmutableMap.of());

        metadata = new MockMetadata(TEST_CATALOG_NAME);
        plannerContext = plannerContextBuilder().withMetadata(metadata).build();
        Map<String, PropertyMetadata<?>> columnProperties = ImmutableMap.of(
                COLUMN_PROPERTY_NAME, longProperty(COLUMN_PROPERTY_NAME, "column_property 1", COLUMN_PROPERTY_DEFAULT_VALUE, false));
        columnPropertyManager = new ColumnPropertyManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, columnProperties));
        Map<String, PropertyMetadata<?>> properties = ImmutableMap.of(
                MATERIALIZED_VIEW_PROPERTY_1_NAME, longProperty(MATERIALIZED_VIEW_PROPERTY_1_NAME, "property 1", MATERIALIZED_VIEW_PROPERTY_1_DEFAULT_VALUE, false),
                MATERIALIZED_VIEW_PROPERTY_2_NAME, stringProperty(MATERIALIZED_VIEW_PROPERTY_2_NAME, "property 2", MATERIALIZED_VIEW_PROPERTY_2_DEFAULT_VALUE, false));
        materializedViewPropertyManager = new MaterializedViewPropertyManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, properties));
        queryStateMachine = stateMachine(transactionManager, createTestMetadataManager(), new AllowAllAccessControl(), testSession);
    }

    @AfterEach
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        testSession = null;
        metadata = null;
        plannerContext = null;
        materializedViewPropertyManager = null;
        transactionManager = null;
        queryStateMachine = null;
    }

    protected static QualifiedObjectName qualifiedObjectName(String objectName)
    {
        return new QualifiedObjectName(TEST_CATALOG_NAME, SCHEMA, objectName);
    }

    protected static QualifiedName qualifiedName(String name)
    {
        return QualifiedName.of(TEST_CATALOG_NAME, SCHEMA, name);
    }

    protected static QualifiedName qualifiedColumnName(String tableName, String columnName)
    {
        return QualifiedName.of(TEST_CATALOG_NAME, SCHEMA, tableName, columnName);
    }

    protected static QualifiedObjectName asQualifiedObjectName(QualifiedName viewName)
    {
        return QualifiedObjectName.valueOf(viewName.toString());
    }

    protected static QualifiedName asQualifiedName(QualifiedObjectName qualifiedObjectName)
    {
        return QualifiedName.of(qualifiedObjectName.catalogName(), qualifiedObjectName.schemaName(), qualifiedObjectName.objectName());
    }

    protected MaterializedViewDefinition someMaterializedView()
    {
        return someMaterializedView("select * from some_table", ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId(), Optional.empty())));
    }

    protected MaterializedViewDefinition someMaterializedView(String sql, List<ViewColumn> columns)
    {
        return new MaterializedViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                columns,
                Optional.empty(),
                Optional.empty(),
                Identity.ofUser("owner"),
                ImmutableList.of(),
                Optional.empty());
    }

    protected static ConnectorTableMetadata someTable(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(new ColumnMetadata("test", BIGINT)));
    }

    protected static ViewDefinition someView()
    {
        return viewDefinition("SELECT 1", ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId(), Optional.empty())));
    }

    protected static ViewDefinition viewDefinition(String sql, ImmutableList<ViewColumn> columns)
    {
        return new ViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                columns,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    private static QueryStateMachine stateMachine(TransactionManager transactionManager, MetadataManager metadata, AccessControl accessControl, Session session)
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                directExecutor(),
                metadata,
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.empty(),
                true,
                new NodeVersion("test"));
    }

    protected static class MockMetadata
            extends AbstractMockMetadata
    {
        private final MetadataManager delegate;
        private final String catalogName;
        private final List<CatalogSchemaName> schemas = new CopyOnWriteArrayList<>();
        private final AtomicBoolean failCreateSchema = new AtomicBoolean();
        private final Map<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, ViewDefinition> views = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, MaterializedViewDefinition> materializedViews = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, Map<String, Object>> materializedViewProperties = new ConcurrentHashMap<>();

        public MockMetadata(String catalogName)
        {
            delegate = createTestMetadataManager();
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @Override
        public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
        {
            if (this.catalogName.equals(catalogName)) {
                return Optional.of(TEST_CATALOG_HANDLE);
            }
            return Optional.empty();
        }

        public void failCreateSchema()
        {
            failCreateSchema.set(true);
        }

        @Override
        public boolean schemaExists(Session session, CatalogSchemaName schema)
        {
            return schemas.contains(schema);
        }

        @Override
        public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
        {
            if (failCreateSchema.get()) {
                throw new TrinoException(DIVISION_BY_ZERO, "TEST create schema fail: " + schema);
            }
            if (schemas.contains(schema)) {
                throw new TrinoException(ALREADY_EXISTS, "Schema already exists");
            }
            schemas.add(schema);
        }

        @Override
        public void dropSchema(Session session, CatalogSchemaName schema, boolean cascade)
        {
            if (cascade) {
                tables.keySet().stream()
                        .filter(table -> schema.getSchemaName().equals(table.getSchemaName()))
                        .forEach(tables::remove);
                views.keySet().stream()
                        .filter(view -> schema.getSchemaName().equals(view.getSchemaName()))
                        .forEach(tables::remove);
                materializedViews.keySet().stream()
                        .filter(materializedView -> schema.getSchemaName().equals(materializedView.getSchemaName()))
                        .forEach(tables::remove);
            }
            schemas.remove(schema);
        }

        @Override
        public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
        {
            List<QualifiedObjectName> tables = ImmutableList.<QualifiedObjectName>builder()
                    .addAll(this.tables.keySet().stream().map(table -> new QualifiedObjectName(catalogName, table.getSchemaName(), table.getTableName())).collect(toImmutableList()))
                    .addAll(this.views.keySet().stream().map(view -> new QualifiedObjectName(catalogName, view.getSchemaName(), view.getTableName())).collect(toImmutableList()))
                    .addAll(this.materializedViews.keySet().stream().map(mv -> new QualifiedObjectName(catalogName, mv.getSchemaName(), mv.getTableName())).collect(toImmutableList()))
                    .build();
            return tables.stream().filter(prefix::matches).collect(toImmutableList());
        }

        @Override
        public TableSchema getTableSchema(Session session, TableHandle tableHandle)
        {
            return new TableSchema(new CatalogName(TEST_CATALOG_NAME), getTableMetadata(tableHandle).getTableSchema());
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            return Optional.ofNullable(tables.get(tableName.asSchemaTableName()))
                    .map(tableMetadata -> new TableHandle(
                            TEST_CATALOG_HANDLE,
                            new TestingTableHandle(tableName.asSchemaTableName()),
                            TestingConnectorTransactionHandle.INSTANCE));
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return new TableMetadata(new CatalogName(TEST_CATALOG_NAME), getTableMetadata(tableHandle));
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
        {
            checkArgument(saveMode == REPLACE || saveMode == IGNORE || !tables.containsKey(tableMetadata.getTable()));
            tables.put(tableMetadata.getTable(), tableMetadata);
        }

        @Override
        public void dropTable(Session session, TableHandle tableHandle, CatalogSchemaTableName tableName)
        {
            tables.remove(tableName.getSchemaTableName());
        }

        @Override
        public void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName currentTableName, QualifiedObjectName newTableName)
        {
            SchemaTableName oldTableName = currentTableName.getSchemaTableName();
            tables.put(newTableName.asSchemaTableName(), verifyNotNull(tables.get(oldTableName), "Table not found %s", oldTableName));
            tables.remove(oldTableName);
        }

        @Override
        public Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
        {
            return Optional.empty();
        }

        @Override
        public void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column, ColumnPosition position)
        {
            SchemaTableName tableName = table.getSchemaTableName();
            ConnectorTableMetadata metadata = tables.get(tableName);

            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builderWithExpectedSize(metadata.getColumns().size() + 1);
            switch (position) {
                case ColumnPosition.First _ -> {
                    columns.add(column);
                    columns.addAll(metadata.getColumns());
                }
                case ColumnPosition.After after -> {
                    for (ColumnMetadata existingColumn : metadata.getColumns()) {
                        columns.add(existingColumn);
                        if (existingColumn.getName().equals(after.columnName())) {
                            columns.add(column);
                        }
                    }
                }
                case ColumnPosition.Last _ -> {
                    columns.addAll(metadata.getColumns());
                    columns.add(column);
                }
            }
            tables.put(tableName, new ConnectorTableMetadata(tableName, columns.build()));
        }

        @Override
        public void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle columnHandle)
        {
            SchemaTableName tableName = table.getSchemaTableName();
            ConnectorTableMetadata metadata = tables.get(tableName);
            String columnName = ((TestingColumnHandle) columnHandle).getName();

            List<ColumnMetadata> columns = metadata.getColumns().stream()
                    .filter(column -> !column.getName().equals(columnName))
                    .collect(toImmutableList());
            tables.put(tableName, new ConnectorTableMetadata(tableName, columns));
        }

        @Override
        public void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target)
        {
            SchemaTableName tableName = table.getSchemaTableName();
            ConnectorTableMetadata metadata = tables.get(tableName);
            String columnName = ((TestingColumnHandle) source).getName();

            List<ColumnMetadata> columns = metadata.getColumns().stream()
                    .map(column -> column.getName().equals(columnName) ? ColumnMetadata.builderFrom(column).setName(target).build() : column)
                    .collect(toImmutableList());
            tables.put(tableName, new ConnectorTableMetadata(tableName, columns));
        }

        @Override
        public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle columnHandle, Type type)
        {
            SchemaTableName tableName = getTableName(tableHandle);
            ConnectorTableMetadata metadata = tables.get(tableName);

            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builderWithExpectedSize(metadata.getColumns().size());
            for (ColumnMetadata column : metadata.getColumns()) {
                if (column.getName().equals(((TestingColumnHandle) columnHandle).getName())) {
                    columns.add(new ColumnMetadata(column.getName(), type));
                }
                else {
                    columns.add(column);
                }
            }

            tables.put(tableName, new ConnectorTableMetadata(tableName, columns.build()));
        }

        @Override
        public void dropNotNullConstraint(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            SchemaTableName tableName = getTableName(tableHandle);
            ConnectorTableMetadata metadata = tables.get(tableName);
            String columnName = ((TestingColumnHandle) columnHandle).getName();

            List<ColumnMetadata> columns = metadata.getColumns().stream()
                    .map(column -> column.getName().equals(columnName) ? ColumnMetadata.builderFrom(column).setNullable(true).build() : column)
                    .collect(toImmutableList());
            tables.put(tableName, new ConnectorTableMetadata(tableName, columns));
        }

        private ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
        {
            return tables.get(getTableName(tableHandle));
        }

        private SchemaTableName getTableName(TableHandle tableHandle)
        {
            return ((TestingTableHandle) tableHandle.connectorHandle()).getTableName();
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            return getTableMetadata(tableHandle).getColumns().stream()
                    .collect(toImmutableMap(
                            ColumnMetadata::getName,
                            column -> new TestingColumnHandle(column.getName())));
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            String columnName = ((TestingColumnHandle) columnHandle).getName();
            return getTableMetadata(tableHandle).getColumns().stream()
                    .filter(column -> column.getName().equals(columnName))
                    .collect(onlyElement());
        }

        @Override
        public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return Optional.ofNullable(materializedViews.get(viewName.asSchemaTableName()));
        }

        @Override
        public Map<String, Object> getMaterializedViewProperties(Session session, QualifiedObjectName viewName, MaterializedViewDefinition materializedViewDefinition)
        {
            return materializedViewProperties.get(viewName.asSchemaTableName());
        }

        @Override
        public void createMaterializedView(
                Session session,
                QualifiedObjectName viewName,
                MaterializedViewDefinition definition,
                Map<String, Object> properties,
                boolean replace,
                boolean ignoreExisting)
        {
            checkArgument(ignoreExisting || !materializedViews.containsKey(viewName.asSchemaTableName()));
            materializedViews.put(viewName.asSchemaTableName(), definition);
            materializedViewProperties.put(viewName.asSchemaTableName(), properties);
        }

        @Override
        public synchronized void setMaterializedViewProperties(
                Session session,
                QualifiedObjectName viewName,
                Map<String, Optional<Object>> properties)
        {
            Map<String, Object> newProperties = new HashMap<>(materializedViewProperties.getOrDefault(viewName.asSchemaTableName(), ImmutableMap.of()));
            for (Entry<String, Optional<Object>> entry : properties.entrySet()) {
                if (entry.getValue().isPresent()) {
                    newProperties.put(entry.getKey(), entry.getValue().orElseThrow());
                }
                else {
                    newProperties.remove(entry.getKey());
                }
            }
            materializedViewProperties.put(viewName.asSchemaTableName(), newProperties);
        }

        @Override
        public void setMaterializedViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
        {
            MaterializedViewDefinition view = materializedViews.get(viewName.asSchemaTableName());
            materializedViews.put(
                    viewName.asSchemaTableName(),
                    new MaterializedViewDefinition(
                            view.getOriginalSql(),
                            view.getCatalog(),
                            view.getSchema(),
                            view.getColumns().stream()
                                    .map(currentViewColumn -> columnName.equals(currentViewColumn.name()) ? new ViewColumn(currentViewColumn.name(), currentViewColumn.type(), comment) : currentViewColumn)
                                    .collect(toImmutableList()),
                            view.getGracePeriod(),
                            view.getComment(),
                            view.getRunAsIdentity().get(),
                            view.getPath(),
                            view.getStorageTable()));
        }

        @Override
        public void dropMaterializedView(Session session, QualifiedObjectName viewName)
        {
            materializedViews.remove(viewName.asSchemaTableName());
        }

        @Override
        public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
        {
            return Optional.ofNullable(views.get(viewName.asSchemaTableName()));
        }

        @Override
        public boolean isView(Session session, QualifiedObjectName viewName)
        {
            return getView(session, viewName).isPresent();
        }

        @Override
        public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
        {
            checkArgument(replace || !views.containsKey(viewName.asSchemaTableName()));
            views.put(viewName.asSchemaTableName(), definition);
        }

        @Override
        public void dropView(Session session, QualifiedObjectName viewName)
        {
            views.remove(viewName.asSchemaTableName());
        }

        @Override
        public void renameView(Session session, QualifiedObjectName source, QualifiedObjectName target)
        {
            SchemaTableName oldViewName = source.asSchemaTableName();
            views.put(target.asSchemaTableName(), verifyNotNull(views.get(oldViewName), "View not found %s", oldViewName));
            views.remove(oldViewName);
        }

        @Override
        public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
        {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
            ConnectorTableMetadata newTableMetadata = new ConnectorTableMetadata(
                    tableMetadata.getTable(),
                    tableMetadata.getColumns(),
                    tableMetadata.getProperties(),
                    comment);
            tables.put(tableMetadata.getTable(), newTableMetadata);
        }

        @Override
        public void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment)
        {
            ViewDefinition view = views.get(viewName.asSchemaTableName());
            views.put(
                    viewName.asSchemaTableName(),
                    new ViewDefinition(
                            view.getOriginalSql(),
                            view.getCatalog(),
                            view.getSchema(),
                            view.getColumns(),
                            comment,
                            view.getRunAsIdentity(),
                            view.getPath()));
        }

        @Override
        public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
        {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
            TestingColumnHandle columnHandle = (TestingColumnHandle) column;
            ConnectorTableMetadata newTableMetadata = new ConnectorTableMetadata(
                    tableMetadata.getTable(),
                    tableMetadata.getColumns().stream()
                            .map(tableColumn -> Objects.equals(tableColumn.getName(), columnHandle.getName()) ? withComment(tableColumn, comment) : tableColumn)
                            .collect(toImmutableList()),
                    tableMetadata.getProperties(),
                    tableMetadata.getComment());
            tables.put(tableMetadata.getTable(), newTableMetadata);
        }

        @Override
        public void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
        {
            ViewDefinition view = views.get(viewName.asSchemaTableName());
            views.put(
                    viewName.asSchemaTableName(),
                    new ViewDefinition(
                            view.getOriginalSql(),
                            view.getCatalog(),
                            view.getSchema(),
                            view.getColumns().stream()
                                    .map(currentViewColumn -> columnName.equals(currentViewColumn.name()) ? new ViewColumn(currentViewColumn.name(), currentViewColumn.type(), comment) : currentViewColumn)
                                    .collect(toImmutableList()),
                            view.getComment(),
                            view.getRunAsIdentity(),
                            view.getPath()));
        }

        @Override
        public void renameMaterializedView(Session session, QualifiedObjectName source, QualifiedObjectName target)
        {
            SchemaTableName oldViewName = source.asSchemaTableName();
            materializedViews.put(target.asSchemaTableName(), verifyNotNull(materializedViews.get(oldViewName), "Materialized View not found %s", oldViewName));
            materializedViews.remove(oldViewName);
        }

        @Override
        public ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType)
        {
            return delegate.getCoercion(operatorType, fromType, toType);
        }

        private static ColumnMetadata withComment(ColumnMetadata tableColumn, Optional<String> comment)
        {
            return ColumnMetadata.builderFrom(tableColumn)
                    .setComment(comment)
                    .build();
        }
    }
}
