
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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.AbstractMockMetadata;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.ColumnDefinition;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.TableElement;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.testing.TestingSession.createBogusTestingCatalog;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCreateTableTask
{
    private static final String CATALOG_NAME = "catalog";
    private CatalogManager catalogManager;
    private TypeManager typeManager;
    private TransactionManager transactionManager;
    private TablePropertyManager tablePropertyManager;
    private ColumnPropertyManager columnPropertyManager;
    private Catalog testCatalog;
    private Session testSession;
    private MockMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        catalogManager = new CatalogManager();
        typeManager = new TypeRegistry();
        transactionManager = createTestTransactionManager(catalogManager);
        tablePropertyManager = new TablePropertyManager();
        columnPropertyManager = new ColumnPropertyManager();
        testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        tablePropertyManager.addProperties(testCatalog.getConnectorCatalogName(),
                ImmutableList.of(stringProperty("baz", "test property", null, false)));
        columnPropertyManager.addProperties(testCatalog.getConnectorCatalogName(), ImmutableList.of());
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new MockMetadata(typeManager,
                tablePropertyManager,
                columnPropertyManager,
                testCatalog.getConnectorCatalogName(),
                emptySet());
    }

    @Test
    public void testCreateTableNotExistsTrue()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), "BIGINT", true, emptyList(), Optional.empty())),
                true,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateTableCallCount(), 1);
    }

    @Test
    public void testCreateTableNotExistsFalse()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), "BIGINT", true, emptyList(), Optional.empty())),
                false,
                ImmutableList.of(),
                Optional.empty());

        try {
            getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
            fail("expected exception");
        }
        catch (RuntimeException e) {
            // Expected
            assertTrue(e instanceof PrestoException);
            PrestoException prestoException = (PrestoException) e;
            assertEquals(prestoException.getErrorCode(), ALREADY_EXISTS.toErrorCode());
        }
        assertEquals(metadata.getCreateTableCallCount(), 1);
    }

    @Test
    public void testCreateWithNotNullColumns()
    {
        metadata.setConnectorCapabilities(NOT_NULL_COLUMN_CONSTRAINT);
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(identifier("a"), "DATE", true, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("b"), "VARCHAR", false, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("c"), "VARBINARY", false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"), inputColumns, true, ImmutableList.of(), Optional.empty());

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateTableCallCount(), 1);
        List<ColumnMetadata> columns = metadata.getReceivedTableMetadata().get(0).getColumns();
        assertEquals(columns.size(), 3);

        assertEquals(columns.get(0).getName(), "a");
        assertEquals(columns.get(0).getType().getDisplayName().toUpperCase(ENGLISH), "DATE");
        assertTrue(columns.get(0).isNullable());

        assertEquals(columns.get(1).getName(), "b");
        assertEquals(columns.get(1).getType().getDisplayName().toUpperCase(ENGLISH), "VARCHAR");
        assertFalse(columns.get(1).isNullable());

        assertEquals(columns.get(2).getName(), "c");
        assertEquals(columns.get(2).getType().getDisplayName().toUpperCase(ENGLISH), "VARBINARY");
        assertFalse(columns.get(2).isNullable());
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*does not support non-null column for column name 'b'")
    public void testCreateWithUnsupportedConnectorThrowsWhenNotNull()
    {
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(identifier("a"), "DATE", true, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("b"), "VARCHAR", false, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("c"), "VARBINARY", false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(
                QualifiedName.of("test_table"),
                inputColumns,
                true,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final TypeManager typeManager;
        private final TablePropertyManager tablePropertyManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final CatalogName catalogHandle;
        private final List<ConnectorTableMetadata> tables = new CopyOnWriteArrayList<>();
        private Set<ConnectorCapabilities> connectorCapabilities;

        public MockMetadata(
                TypeManager typeManager,
                TablePropertyManager tablePropertyManager,
                ColumnPropertyManager columnPropertyManager,
                CatalogName catalogHandle,
                Set<ConnectorCapabilities> connectorCapabilities)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.connectorCapabilities = requireNonNull(immutableEnumSet(connectorCapabilities), "connectorCapabilities is null");
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
        {
            tables.add(tableMetadata);
            if (!ignoreExisting) {
                throw new PrestoException(ALREADY_EXISTS, "Table already exists");
            }
        }

        @Override
        public TablePropertyManager getTablePropertyManager()
        {
            return tablePropertyManager;
        }

        @Override
        public ColumnPropertyManager getColumnPropertyManager()
        {
            return columnPropertyManager;
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return typeManager.getType(signature);
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
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            return Optional.empty();
        }

        public int getCreateTableCallCount()
        {
            return tables.size();
        }

        public List<ConnectorTableMetadata> getReceivedTableMetadata()
        {
            return tables;
        }

        @Override
        public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogName catalogName)
        {
            return connectorCapabilities;
        }

        public void setConnectorCapabilities(ConnectorCapabilities... connectorCapabilities)
        {
            this.connectorCapabilities = immutableEnumSet(ImmutableList.copyOf(connectorCapabilities));
        }
    }
}
