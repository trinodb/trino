
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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TableElement;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCreateTableTask
{
    private static final String OTHER_CATALOG_NAME = "other_catalog";
    private static final ConnectorTableMetadata PARENT_TABLE = new ConnectorTableMetadata(
            new SchemaTableName("schema", "parent_table"),
            List.of(new ColumnMetadata("a", SMALLINT), new ColumnMetadata("b", BIGINT)),
            Map.of("baz", "property_value"));

    private LocalQueryRunner queryRunner;
    private Session testSession;
    private MockMetadata metadata;
    private PlannerContext plannerContext;
    private TransactionManager transactionManager;
    private ColumnPropertyManager columnPropertyManager;
    private TablePropertyManager tablePropertyManager;
    private CatalogHandle testCatalogHandle;
    private CatalogHandle otherCatalogHandle;

    @BeforeMethod
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .build());
        transactionManager = queryRunner.getTransactionManager();
        queryRunner.createCatalog(
                TEST_CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withTableProperties(() -> ImmutableList.of(stringProperty("baz", "test property", null, false)))
                        .build(),
                ImmutableMap.of());
        testCatalogHandle = queryRunner.getCatalogHandle(TEST_CATALOG_NAME);
        queryRunner.createCatalog(
                OTHER_CATALOG_NAME,
                MockConnectorFactory.builder().withName("other_mock").build(),
                ImmutableMap.of());
        otherCatalogHandle = queryRunner.getCatalogHandle(OTHER_CATALOG_NAME);

        tablePropertyManager = queryRunner.getTablePropertyManager();
        columnPropertyManager = queryRunner.getColumnPropertyManager();
        testSession = Session.builder(queryRunner.getDefaultSession())
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new MockMetadata();
        plannerContext = plannerContextBuilder().withMetadata(metadata).build();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        queryRunner = null;
        transactionManager = null;
        tablePropertyManager = null;
        columnPropertyManager = null;
        metadata = null;
        plannerContext = null;
    }

    @Test
    public void testCreateTableNotExistsTrue()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), toSqlType(BIGINT), true, emptyList(), Optional.empty())),
                true,
                ImmutableList.of(),
                Optional.empty());

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        getFutureValue(createTableTask.internalExecute(statement, testSession, emptyList(), output -> {}));
        assertEquals(metadata.getCreateTableCallCount(), 1);
    }

    @Test
    public void testCreateTableNotExistsFalse()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), toSqlType(BIGINT), true, emptyList(), Optional.empty())),
                false,
                ImmutableList.of(),
                Optional.empty());

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        assertTrinoExceptionThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, testSession, emptyList(), output -> {})))
                .hasErrorCode(ALREADY_EXISTS)
                .hasMessage("Table already exists");

        assertEquals(metadata.getCreateTableCallCount(), 1);
    }

    @Test
    public void testCreateTableWithMaterializedViewPropertyFails()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), toSqlType(BIGINT), true, emptyList(), Optional.empty())),
                false,
                ImmutableList.of(new Property(new Identifier("foo"), new StringLiteral("bar"))),
                Optional.empty());

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        assertTrinoExceptionThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, testSession, emptyList(), output -> {})))
                .hasErrorCode(INVALID_TABLE_PROPERTY)
                .hasMessage("Catalog 'test-catalog' table property 'foo' does not exist");

        assertEquals(metadata.getCreateTableCallCount(), 0);
    }

    @Test
    public void testCreateWithNotNullColumns()
    {
        metadata.setConnectorCapabilities(NOT_NULL_COLUMN_CONSTRAINT);
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(identifier("a"), toSqlType(DATE), true, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("b"), toSqlType(VARCHAR), false, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("c"), toSqlType(VARBINARY), false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"), inputColumns, true, ImmutableList.of(), Optional.empty());

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        getFutureValue(createTableTask.internalExecute(statement, testSession, emptyList(), output -> {}));
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

    @Test
    public void testCreateWithUnsupportedConnectorThrowsWhenNotNull()
    {
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(identifier("a"), toSqlType(DATE), true, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("b"), toSqlType(VARCHAR), false, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("c"), toSqlType(VARBINARY), false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(
                QualifiedName.of("test_table"),
                inputColumns,
                true,
                ImmutableList.of(),
                Optional.empty());

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        assertTrinoExceptionThrownBy(() ->
                getFutureValue(createTableTask.internalExecute(statement, testSession, emptyList(), output -> {})))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Catalog 'test-catalog' does not support non-null column for column name 'b'");
    }

    @Test
    public void testCreateLike()
    {
        CreateTable statement = getCreateLikeStatement(false);

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        getFutureValue(createTableTask.internalExecute(statement, testSession, List.of(), output -> {}));
        assertEquals(metadata.getCreateTableCallCount(), 1);

        assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                .isEqualTo(PARENT_TABLE.getColumns());
        assertThat(metadata.getReceivedTableMetadata().get(0).getProperties()).isEmpty();
    }

    @Test
    public void testCreateLikeIncludingProperties()
    {
        CreateTable statement = getCreateLikeStatement(true);

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        getFutureValue(createTableTask.internalExecute(statement, testSession, List.of(), output -> {}));
        assertEquals(metadata.getCreateTableCallCount(), 1);

        assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                .isEqualTo(PARENT_TABLE.getColumns());
        assertThat(metadata.getReceivedTableMetadata().get(0).getProperties())
                .isEqualTo(PARENT_TABLE.getProperties());
    }

    @Test
    public void testCreateLikeExcludingPropertiesAcrossCatalogs()
    {
        CreateTable statement = getCreateLikeStatement(QualifiedName.of(OTHER_CATALOG_NAME, "other_schema", "test_table"), false);

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        getFutureValue(createTableTask.internalExecute(statement, testSession, List.of(), output -> {}));
        assertEquals(metadata.getCreateTableCallCount(), 1);

        assertThat(metadata.getReceivedTableMetadata().get(0).getColumns())
                .isEqualTo(PARENT_TABLE.getColumns());
    }

    @Test
    public void testCreateLikeIncludingPropertiesAcrossCatalogs()
    {
        CreateTable failingStatement = getCreateLikeStatement(QualifiedName.of(OTHER_CATALOG_NAME, "other_schema", "test_table"), true);

        CreateTableTask failingCreateTableTask = new CreateTableTask(plannerContext, new AllowAllAccessControl(), columnPropertyManager, tablePropertyManager);
        assertThatThrownBy(() -> getFutureValue(failingCreateTableTask.internalExecute(failingStatement, testSession, List.of(), output -> {})))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("CREATE TABLE LIKE table INCLUDING PROPERTIES across catalogs is not supported");
    }

    @Test
    public void testCreateLikeDenyPermission()
    {
        CreateTable statement = getCreateLikeStatement(false);

        TestingAccessControlManager accessControl = new TestingAccessControlManager(transactionManager, new EventListenerManager(new EventListenerConfig()));
        accessControl.deny(privilege("parent_table", SELECT_COLUMN));

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, accessControl, columnPropertyManager, tablePropertyManager);
        assertThatThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, testSession, List.of(), output -> {})))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Cannot reference columns of table");
    }

    @Test
    public void testCreateLikeIncludingPropertiesDenyPermission()
    {
        CreateTable statement = getCreateLikeStatement(true);

        TestingAccessControlManager accessControl = new TestingAccessControlManager(transactionManager, new EventListenerManager(new EventListenerConfig()));
        accessControl.deny(privilege("parent_table", SHOW_CREATE_TABLE));

        CreateTableTask createTableTask = new CreateTableTask(plannerContext, accessControl, columnPropertyManager, tablePropertyManager);
        assertThatThrownBy(() -> getFutureValue(createTableTask.internalExecute(statement, testSession, List.of(), output -> {})))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Cannot reference properties of table");
    }

    private static CreateTable getCreateLikeStatement(boolean includingProperties)
    {
        return getCreateLikeStatement(QualifiedName.of("test_table"), includingProperties);
    }

    private static CreateTable getCreateLikeStatement(QualifiedName name, boolean includingProperties)
    {
        return new CreateTable(
                name,
                List.of(new LikeClause(QualifiedName.of(PARENT_TABLE.getTable().getTableName()), includingProperties ? Optional.of(INCLUDING) : Optional.empty())),
                true,
                ImmutableList.of(),
                Optional.empty());
    }

    private class MockMetadata
            extends AbstractMockMetadata
    {
        private final List<ConnectorTableMetadata> tables = new CopyOnWriteArrayList<>();
        private Set<ConnectorCapabilities> connectorCapabilities = ImmutableSet.of();

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
        {
            tables.add(tableMetadata);
            if (!ignoreExisting) {
                throw new TrinoException(ALREADY_EXISTS, "Table already exists");
            }
        }

        @Override
        public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
        {
            if (catalogName.equals(TEST_CATALOG_NAME)) {
                return Optional.of(testCatalogHandle);
            }
            if (catalogName.equals(OTHER_CATALOG_NAME)) {
                return Optional.of(otherCatalogHandle);
            }
            return Optional.empty();
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            if (tableName.asSchemaTableName().equals(PARENT_TABLE.getTable())) {
                return Optional.of(
                        new TableHandle(
                                TEST_CATALOG_HANDLE,
                                new TestingTableHandle(tableName.asSchemaTableName()),
                                TestingConnectorTransactionHandle.INSTANCE));
            }
            return Optional.empty();
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            if ((tableHandle.getConnectorHandle() instanceof TestingTableHandle)) {
                if (((TestingTableHandle) tableHandle.getConnectorHandle()).getTableName().equals(PARENT_TABLE.getTable())) {
                    return new TableMetadata(TEST_CATALOG_NAME, PARENT_TABLE);
                }
            }

            return super.getTableMetadata(session, tableHandle);
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
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
        {
            return connectorCapabilities;
        }

        public void setConnectorCapabilities(ConnectorCapabilities... connectorCapabilities)
        {
            this.connectorCapabilities = immutableEnumSet(ImmutableList.copyOf(connectorCapabilities));
        }
    }
}
