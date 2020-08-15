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
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TableSchema;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

@Test
public abstract class BaseDataDefinitionTaskTest
{
    protected static final String CATALOG_NAME = "catalog";
    public static final String SCHEMA = "schema";

    protected static final String MATERIALIZED_VIEW_PROPERTY_1_NAME = "property1";
    protected static final Long MATERIALIZED_VIEW_PROPERTY_1_DEFAULT_VALUE = null;

    protected static final String MATERIALIZED_VIEW_PROPERTY_2_NAME = "property2";
    protected static final String MATERIALIZED_VIEW_PROPERTY_2_DEFAULT_VALUE = "defaultProperty2Value";

    private LocalQueryRunner queryRunner;
    protected Session testSession;
    protected MockMetadata metadata;
    protected PlannerContext plannerContext;
    protected MaterializedViewPropertyManager materializedViewPropertyManager;
    protected TransactionManager transactionManager;
    protected QueryStateMachine queryStateMachine;

    @BeforeMethod
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        transactionManager = queryRunner.getTransactionManager();
        queryRunner.createCatalog(CATALOG_NAME, MockConnectorFactory.create("initial"), ImmutableMap.of());

        testSession = testSessionBuilder().build();
        metadata = new MockMetadata(new CatalogName(CATALOG_NAME));
        plannerContext = plannerContextBuilder().withMetadata(metadata).build();
        materializedViewPropertyManager = new MaterializedViewPropertyManager();
        materializedViewPropertyManager.addProperties(
                new CatalogName(CATALOG_NAME),
                ImmutableList.of(
                        longProperty(MATERIALIZED_VIEW_PROPERTY_1_NAME, "property 1", MATERIALIZED_VIEW_PROPERTY_1_DEFAULT_VALUE, false),
                        stringProperty(MATERIALIZED_VIEW_PROPERTY_2_NAME, "property 2", MATERIALIZED_VIEW_PROPERTY_2_DEFAULT_VALUE, false)));
        queryStateMachine = stateMachine(transactionManager, createTestMetadataManager(), new AllowAllAccessControl(), testSession);
    }

    @AfterMethod
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    protected static QualifiedObjectName qualifiedObjectName(String objectName)
    {
        return new QualifiedObjectName(CATALOG_NAME, SCHEMA, objectName);
    }

    protected static QualifiedName qualifiedName(String name)
    {
        return QualifiedName.of(CATALOG_NAME, SCHEMA, name);
    }

    protected static QualifiedObjectName asQualifiedObjectName(QualifiedName viewName)
    {
        return QualifiedObjectName.valueOf(viewName.toString());
    }

    protected static QualifiedName asQualifiedName(QualifiedObjectName qualifiedObjectName)
    {
        return QualifiedName.of(qualifiedObjectName.getCatalogName(), qualifiedObjectName.getSchemaName(), qualifiedObjectName.getObjectName());
    }

    protected MaterializedViewDefinition someMaterializedView()
    {
        return someMaterializedView("select * from some_table", ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId())));
    }

    protected MaterializedViewDefinition someMaterializedView(String sql, List<ViewColumn> columns)
    {
        return new MaterializedViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                columns,
                Optional.empty(),
                Identity.ofUser("owner"),
                Optional.empty(),
                ImmutableMap.of(MATERIALIZED_VIEW_PROPERTY_2_NAME, MATERIALIZED_VIEW_PROPERTY_2_DEFAULT_VALUE));
    }

    protected static ConnectorTableMetadata someTable(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(new ColumnMetadata("test", BIGINT)));
    }

    protected static ViewDefinition someView()
    {
        return viewDefinition("SELECT 1", ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId())));
    }

    protected static ViewDefinition viewDefinition(String sql, ImmutableList<ViewColumn> columns)
    {
        return new ViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                columns,
                Optional.empty(),
                Optional.empty());
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
                Optional.empty());
    }

    protected static class MockMetadata
            extends AbstractMockMetadata
    {
        private final MetadataManager delegate;
        private final CatalogName catalogHandle;
        private final List<CatalogSchemaName> schemas = new CopyOnWriteArrayList<>();
        private final AtomicBoolean failCreateSchema = new AtomicBoolean();
        private final Map<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, ViewDefinition> views = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, MaterializedViewDefinition> materializedViews = new ConcurrentHashMap<>();

        public MockMetadata(CatalogName catalogHandle)
        {
            delegate = createTestMetadataManager();
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        }

        @Override
        public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
        {
            if (catalogHandle.getCatalogName().equals(catalogName)) {
                return Optional.of(catalogHandle);
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
        public TableSchema getTableSchema(Session session, TableHandle tableHandle)
        {
            return new TableSchema(tableHandle.getCatalogName(), getTableMetadata(tableHandle).getTableSchema());
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            return Optional.ofNullable(tables.get(tableName.asSchemaTableName()))
                    .map(tableMetadata -> new TableHandle(
                            new CatalogName(CATALOG_NAME),
                            new TestingTableHandle(tableName.asSchemaTableName()),
                            TestingConnectorTransactionHandle.INSTANCE));
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return new TableMetadata(new CatalogName("catalog"), getTableMetadata(tableHandle));
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
        {
            checkArgument(ignoreExisting || !tables.containsKey(tableMetadata.getTable()));
            tables.put(tableMetadata.getTable(), tableMetadata);
        }

        @Override
        public void dropTable(Session session, TableHandle tableHandle)
        {
            tables.remove(getTableName(tableHandle));
        }

        @Override
        public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
        {
            SchemaTableName oldTableName = getTableName(tableHandle);
            tables.put(newTableName.asSchemaTableName(), verifyNotNull(tables.get(oldTableName), "Table not found %s", oldTableName));
            tables.remove(oldTableName);
        }

        private ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
        {
            return tables.get(getTableName(tableHandle));
        }

        private SchemaTableName getTableName(TableHandle tableHandle)
        {
            return ((TestingTableHandle) tableHandle.getConnectorHandle()).getTableName();
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
        public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return Optional.ofNullable(materializedViews.get(viewName.asSchemaTableName()));
        }

        @Override
        public void createMaterializedView(Session session, QualifiedObjectName viewName, MaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
        {
            checkArgument(ignoreExisting || !materializedViews.containsKey(viewName.asSchemaTableName()));
            materializedViews.put(viewName.asSchemaTableName(), definition);
        }

        @Override
        public synchronized void setMaterializedViewProperties(
                Session session,
                QualifiedObjectName viewName,
                Map<String, Optional<Object>> properties)
        {
            MaterializedViewDefinition existingDefinition = getMaterializedView(session, viewName)
                    .orElseThrow(() -> new MaterializedViewNotFoundException(viewName.asSchemaTableName()));
            Map<String, Object> newProperties = new HashMap<>(existingDefinition.getProperties());
            for (Entry<String, Optional<Object>> entry : properties.entrySet()) {
                if (entry.getValue().isPresent()) {
                    newProperties.put(entry.getKey(), entry.getValue().orElseThrow());
                }
                else {
                    newProperties.remove(entry.getKey());
                }
            }
            materializedViews.put(
                    viewName.asSchemaTableName(),
                    new MaterializedViewDefinition(
                            existingDefinition.getOriginalSql(),
                            existingDefinition.getCatalog(),
                            existingDefinition.getSchema(),
                            existingDefinition.getColumns(),
                            existingDefinition.getComment(),
                            existingDefinition.getRunAsIdentity().get(),
                            existingDefinition.getStorageTable(),
                            newProperties));
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
        public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, boolean replace)
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
        public void renameMaterializedView(Session session, QualifiedObjectName source, QualifiedObjectName target)
        {
            SchemaTableName oldViewName = source.asSchemaTableName();
            materializedViews.put(target.asSchemaTableName(), verifyNotNull(materializedViews.get(oldViewName), "Materialized View not found %s", oldViewName));
            materializedViews.remove(oldViewName);
        }

        @Override
        public ResolvedFunction getCoercion(Session session, OperatorType operatorType, Type fromType, Type toType)
        {
            return delegate.getCoercion(session, operatorType, fromType, toType);
        }
    }
}
