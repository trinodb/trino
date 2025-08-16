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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TypeId;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RefreshView;
import io.trino.testing.TestingGroupProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRefreshViewTask
        extends BaseDataDefinitionTaskTest
{
    private SqlParser parser;
    private AnalyzerFactory analyzerFactory;

    @Override
    @BeforeEach
    public void setUp()
    {
        super.setUp();
        parser = new SqlParser();
        analyzerFactory = new AnalyzerFactory(
                createTestingStatementAnalyzerFactory(
                        plannerContext,
                        new AllowAllAccessControl(),
                        new TablePropertyManager(CatalogServiceProvider.fail()),
                        new AnalyzePropertyManager(CatalogServiceProvider.fail())),
                new StatementRewrite(ImmutableSet.of()),
                plannerContext.getTracer());
    }

    @Test
    void testAddNewColumnsOnTableWhenRefreshingExistingView()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        metadata.createView(
                testSession,
                qualifiedObjectName("existing_view"),
                new ViewDefinition(
                        "SELECT * FROM test_catalog.schema.existing_table",
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new ViewColumn("test", TypeId.of("bigint"), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()),
                ImmutableMap.of(),
                false);

        metadata.addColumn(
                testSession,
                metadata.getTableHandle(testSession, tableName).orElseThrow(),
                new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "existing_table"),
                new ColumnMetadata("new_col", BIGINT),
                new io.trino.spi.connector.ColumnPosition.Last());

        getFutureValue(executeRefreshView(asQualifiedName(qualifiedObjectName("existing_view"))));
        assertThat(metadata.getView(testSession, qualifiedObjectName("existing_view")).orElseThrow().getColumns())
                .containsExactly(
                        new ViewColumn("test", TypeId.of("bigint"), Optional.empty()),
                        new ViewColumn("new_col", TypeId.of("bigint"), Optional.empty()));
    }

    @Test
    void testDropColumnsOnTableWhenRefreshingExistingView()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(
                testSession,
                TEST_CATALOG_NAME,
                new ConnectorTableMetadata(
                        tableName.asSchemaTableName(),
                        ImmutableList.of(
                                new ColumnMetadata("test", BIGINT),
                                new ColumnMetadata("column_to_be_dropped", BIGINT))),
                FAIL);
        metadata.createView(
                testSession,
                qualifiedObjectName("existing_view"),
                new ViewDefinition(
                        "SELECT * FROM test_catalog.schema.existing_table",
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                new ViewColumn("test", TypeId.of("bigint"), Optional.empty()),
                                new ViewColumn("column_to_be_dropped", TypeId.of("bigint"), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()),
                ImmutableMap.of(),
                false);

        TableHandle tableHandle = metadata.getTableHandle(testSession, tableName).orElseThrow();
        metadata.dropColumn(
                testSession,
                tableHandle,
                new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "existing_table"),
                metadata.getColumnHandles(testSession, tableHandle).get("column_to_be_dropped"));

        getFutureValue(executeRefreshView(asQualifiedName(qualifiedObjectName("existing_view"))));
        assertThat(metadata.getView(testSession, qualifiedObjectName("existing_view")).orElseThrow().getColumns())
                .containsExactly(new ViewColumn("test", TypeId.of("bigint"), Optional.empty()));
    }

    @Test
    void testRenameColumnsOnTableWhenRefreshingExistingView()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        metadata.createView(
                testSession,
                qualifiedObjectName("existing_view"),
                new ViewDefinition(
                        "SELECT * FROM test_catalog.schema.existing_table",
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new ViewColumn("test", TypeId.of("bigint"), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()),
                ImmutableMap.of(),
                false);

        TableHandle tableHandle = metadata.getTableHandle(testSession, tableName).orElseThrow();
        metadata.renameColumn(
                testSession,
                tableHandle,
                new CatalogSchemaTableName(TEST_CATALOG_NAME, SCHEMA, "existing_table"),
                metadata.getColumnHandles(testSession, tableHandle).get("test"),
                "renamed_column");

        getFutureValue(executeRefreshView(asQualifiedName(qualifiedObjectName("existing_view"))));
        assertThat(metadata.getView(testSession, qualifiedObjectName("existing_view")).orElseThrow().getColumns())
                .containsExactly(new ViewColumn("renamed_column", TypeId.of("bigint"), Optional.empty()));
    }

    @Test
    void testColumnTypeChangeOnTableWhenRefreshingExistingView()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        metadata.createView(
                testSession,
                qualifiedObjectName("existing_view"),
                new ViewDefinition(
                        "SELECT * FROM test_catalog.schema.existing_table",
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new ViewColumn("test", TypeId.of("bigint"), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()),
                ImmutableMap.of(),
                false);

        TableHandle tableHandle = metadata.getTableHandle(testSession, tableName).orElseThrow();
        metadata.setColumnType(
                testSession,
                tableHandle,
                metadata.getColumnHandles(testSession, tableHandle).get("test"),
                VARCHAR);

        getFutureValue(executeRefreshView(asQualifiedName(qualifiedObjectName("existing_view"))));
        assertThat(metadata.getView(testSession, qualifiedObjectName("existing_view")).orElseThrow().getColumns())
                .containsExactly(new ViewColumn("test", TypeId.of("varchar"), Optional.empty()));
    }

    @Test
    void testTableDroppedWhenRefreshingExistingView()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        metadata.createView(
                testSession,
                qualifiedObjectName("existing_view"),
                new ViewDefinition(
                        "SELECT * FROM test_catalog.schema.existing_table",
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new ViewColumn("test", TypeId.of("bigint"), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()),
                ImmutableMap.of(),
                false);

        TableHandle tableHandle = metadata.getTableHandle(testSession, tableName).orElseThrow();
        metadata.dropTable(testSession, tableHandle, tableName.asCatalogSchemaTableName());

        assertThatThrownBy(() -> getFutureValue(executeRefreshView(asQualifiedName(qualifiedObjectName("existing_view")))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("line 1:15: Table 'test_catalog.schema.existing_table' does not exist");
    }

    @Test
    void testRefreshOnInvokerViewWithRevokedAccessForTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        metadata.createView(
                testSession,
                qualifiedObjectName("existing_view"),
                new ViewDefinition(
                        "SELECT * FROM test_catalog.schema.existing_table",
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new ViewColumn("test", TypeId.of("bigint"), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()),
                ImmutableMap.of(),
                false);

        assertThatThrownBy(() -> getFutureValue(
                executeRefreshView(
                        asQualifiedName(qualifiedObjectName("existing_view")),
                        new TestingAccessControl(ImmutableSet.of("existing_table")))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Access Denied: Cannot select from columns [test] in table or view test_catalog.schema.existing_table");
    }

    @Test
    void testRefreshOnDefinerViewWithRevokedAccessForTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        metadata.createView(
                testSession,
                qualifiedObjectName("existing_view"),
                new ViewDefinition(
                        "SELECT * FROM test_catalog.schema.existing_table",
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new ViewColumn("test", TypeId.of("bigint"), Optional.empty())),
                        Optional.empty(),
                        Optional.of(Identity.ofUser("owner")),
                        ImmutableList.of()),
                ImmutableMap.of(),
                false);

        assertThatThrownBy(() -> getFutureValue(
                executeRefreshView(asQualifiedName(
                        qualifiedObjectName("existing_view")),
                        new TestingAccessControl(ImmutableSet.of("existing_table")))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Access Denied: View owner does not have sufficient privileges: Cannot select from columns [test] in table or view test_catalog.schema.existing_table");
    }

    private ListenableFuture<Void> executeRefreshView(QualifiedName viewName)
    {
        return executeRefreshView(viewName, new AllowAllAccessControl());
    }

    private ListenableFuture<Void> executeRefreshView(QualifiedName viewName, AccessControl accessControl)
    {
        RefreshView statement = new RefreshView(new NodeLocation(1, 1), viewName);
        return new RefreshViewTask(
                plannerContext,
                accessControl,
                new TestingGroupProvider(),
                parser,
                analyzerFactory)
                .execute(statement, queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private static class TestingAccessControl
            extends AllowAllAccessControl
    {
        private final Set<String> deniedTables;

        public TestingAccessControl(Set<String> deniedTables)
        {
            this.deniedTables = ImmutableSet.copyOf(requireNonNull(deniedTables, "deniedTables are null"));
        }

        @Override
        public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
        {
            if (deniedTables.contains(tableName.objectName())) {
                denySelectColumns(tableName.toString(), columnNames);
            }
        }

        @Override
        public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
        {
            if (deniedTables.contains(tableName.objectName())) {
                denySelectColumns(tableName.toString(), columnNames);
            }
        }
    }
}
