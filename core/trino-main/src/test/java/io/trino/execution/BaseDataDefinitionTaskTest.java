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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.TableSchema;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.createBogusTestingCatalog;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Objects.requireNonNull;

@Test
public abstract class BaseDataDefinitionTaskTest
{
    protected static final String CATALOG_NAME = "catalog";
    public static final String SCHEMA = "schema";
    protected Session testSession;
    protected MockMetadata metadata;
    protected TransactionManager transactionManager;
    protected QueryStateMachine queryStateMachine;

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        TablePropertyManager tablePropertyManager = new TablePropertyManager();
        MaterializedViewPropertyManager materializedViewPropertyManager = new MaterializedViewPropertyManager();
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new MockMetadata(
                tablePropertyManager,
                materializedViewPropertyManager,
                testCatalog.getConnectorCatalogName());
        queryStateMachine = stateMachine(transactionManager, createTestMetadataManager(), new AllowAllAccessControl(), testSession);
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

    protected ConnectorMaterializedViewDefinition someMaterializedView()
    {
        return someMaterializedView("select * from some_table", ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("test", BIGINT.getTypeId())));
    }

    protected ConnectorMaterializedViewDefinition someMaterializedView(
            String sql,
            List<ConnectorMaterializedViewDefinition.Column> columns)
    {
        return new ConnectorMaterializedViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                columns,
                Optional.empty(),
                "owner",
                ImmutableMap.of());
    }

    protected static ConnectorTableMetadata someTable(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(new ColumnMetadata("test", BIGINT)));
    }

    protected static ConnectorViewDefinition someView()
    {
        return viewDefinition("SELECT 1", ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test", BIGINT.getTypeId())));
    }

    protected static ConnectorViewDefinition viewDefinition(String sql, ImmutableList<ConnectorViewDefinition.ViewColumn> columns)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                columns,
                Optional.empty(),
                Optional.empty(),
                true);
    }

    private static QueryStateMachine stateMachine(TransactionManager transactionManager, MetadataManager metadata, AccessControl accessControl, Session session)
    {
        return QueryStateMachine.begin(
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
        private final TablePropertyManager tablePropertyManager;
        private final MaterializedViewPropertyManager materializedViewPropertyManager;
        private final CatalogName catalogHandle;
        private final Map<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, ConnectorViewDefinition> views = new ConcurrentHashMap<>();
        private final Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new ConcurrentHashMap<>();

        public MockMetadata(
                TablePropertyManager tablePropertyManager,
                MaterializedViewPropertyManager materializedViewPropertyManager,
                CatalogName catalogHandle)
        {
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.materializedViewPropertyManager = requireNonNull(materializedViewPropertyManager, "materializedViewPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        }

        @Override
        public TablePropertyManager getTablePropertyManager()
        {
            return tablePropertyManager;
        }

        @Override
        public MaterializedViewPropertyManager getMaterializedViewPropertyManager()
        {
            return materializedViewPropertyManager;
        }

        @Override
        public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
        {
            if (catalogHandle.getCatalogName().equals(catalogName)) {
                return Optional.of(catalogHandle);
            }
            return Optional.empty();
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
                            TestingConnectorTransactionHandle.INSTANCE,
                            Optional.empty()));
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
        public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return Optional.ofNullable(materializedViews.get(viewName.asSchemaTableName()));
        }

        @Override
        public void createMaterializedView(Session session, QualifiedObjectName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
        {
            checkArgument(ignoreExisting || !materializedViews.containsKey(viewName.asSchemaTableName()));
            materializedViews.put(viewName.asSchemaTableName(), definition);
        }

        @Override
        public void dropMaterializedView(Session session, QualifiedObjectName viewName)
        {
            materializedViews.remove(viewName.asSchemaTableName());
        }

        @Override
        public Optional<ConnectorViewDefinition> getView(Session session, QualifiedObjectName viewName)
        {
            return Optional.ofNullable(views.get(viewName.asSchemaTableName()));
        }

        @Override
        public void createView(Session session, QualifiedObjectName viewName, ConnectorViewDefinition definition, boolean replace)
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
    }
}
