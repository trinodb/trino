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
package io.prestosql.sql.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.NestedColumn;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.TestingHandleResolver;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.TestingPageSinkProvider;
import io.prestosql.testing.TestingSplitManager;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class TestMergeNestedColumns
        extends BasePlanTest
{
    private static final Type MSG_TYPE = RowType.from(ImmutableList.of(field("x", VarcharType.VARCHAR), field("y", VarcharType.VARCHAR)));

    public TestMergeNestedColumns()
    {
        super(TestMergeNestedColumns::createQueryRunner);
    }

    @Test
    public void testSelectDereference()
    {
        assertPlan("select foo.x, foo.y, bar.x, bar.y from nested_column_table",
                output(ImmutableList.of("foo_x", "foo_y", "bar_x", "bar_y"),
                        project(ImmutableMap.of("foo_x", expression("foo_x"), "foo_y", expression("foo_y"), "bar_x", expression("bar.x"), "bar_y", expression("bar.y")),
                                tableScan("nested_column_table", ImmutableMap.of("foo_x", "foo.x", "foo_y", "foo.y", "bar", "bar")))));
    }

    @Test
    public void testSelectDereferenceAndParentDoesNotFire()
    {
        assertPlan("select foo.x, foo.y, foo from nested_column_table",
                output(ImmutableList.of("foo_x", "foo_y", "foo"),
                        project(ImmutableMap.of("foo_x", expression("foo.x"), "foo_y", expression("foo.y"), "foo", expression("foo")),
                                tableScan("nested_column_table", ImmutableMap.of("foo", "foo")))));
    }

    private static LocalQueryRunner createQueryRunner()
    {
        String schemaName = "test-schema";
        String catalogName = "test";
        TableInfo regularTable = new TableInfo(new SchemaTableName(schemaName, "regular_table"),
                ImmutableList.of(new ColumnMetadata("dummy_column", VarcharType.VARCHAR)), ImmutableMap.of());

        TableInfo nestedColumnTable = new TableInfo(new SchemaTableName(schemaName, "nested_column_table"),
                ImmutableList.of(new ColumnMetadata("foo", MSG_TYPE), new ColumnMetadata("bar", MSG_TYPE)), ImmutableMap.of(
                new NestedColumn(ImmutableList.of("foo", "x")), 0,
                new NestedColumn(ImmutableList.of("foo", "y")), 0));

        ImmutableList<TableInfo> tableInfos = ImmutableList.of(regularTable, nestedColumnTable);

        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .build());
        queryRunner.createCatalog(catalogName, new TestConnectorFactory(new TestMetadata(tableInfos)), ImmutableMap.of());
        return queryRunner;
    }

    private static class TestConnectorFactory
            implements ConnectorFactory
    {
        private final TestMetadata metadata;

        public TestConnectorFactory(TestMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new TestingHandleResolver();
        }

        @Override
        public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
        {
            return new TestConnector(metadata);
        }
    }

    private enum TransactionInstance
            implements ConnectorTransactionHandle
    {
        INSTANCE
    }

    private static class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        private TestConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return TransactionInstance.INSTANCE;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TestingSplitManager(ImmutableList.of());
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return (transactionHandle, session, split, columns) -> new FixedPageSource(ImmutableList.of());
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }
    }

    private static class TestMetadata
            extends TestingMetadata
    {
        private final List<TableInfo> tableInfos;

        TestMetadata(List<TableInfo> tableInfos)
        {
            this.tableInfos = requireNonNull(tableInfos, "tableinfos is null");
            insertTables();
        }

        private void insertTables()
        {
            for (TableInfo tableInfo : tableInfos) {
                getTables().put(tableInfo.getSchemaTableName(), tableInfo.getTableMetadata());
            }
        }

        @Override
        public Map<NestedColumn, ColumnHandle> getNestedColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<NestedColumn> dereferences)
        {
            requireNonNull(tableHandle, "tableHandle is null");
            SchemaTableName tableName = getTableName(tableHandle);
            return tableInfos.stream().filter(tableInfo -> tableInfo.getSchemaTableName().equals(tableName)).map(TableInfo::getNestedColumnHandle).findFirst().orElse(ImmutableMap.of());
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
        {
            return new ConnectorTableLayout(new ConnectorTableLayoutHandle() {});
        }

        @Override
        public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
        {
            return ImmutableList.of(new ConnectorTableLayoutResult(new ConnectorTableLayout(new ConnectorTableLayoutHandle() {}), constraint.getSummary()));
        }
    }

    private static class TableInfo
    {
        private final SchemaTableName schemaTableName;
        private final List<ColumnMetadata> columnMetadatas;
        private final Map<NestedColumn, Integer> nestedColumns;

        public TableInfo(SchemaTableName schemaTableName, List<ColumnMetadata> columnMetadata, Map<NestedColumn, Integer> nestedColumns)
        {
            this.schemaTableName = schemaTableName;
            this.columnMetadatas = columnMetadata;
            this.nestedColumns = nestedColumns;
        }

        SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        ConnectorTableMetadata getTableMetadata()
        {
            return new ConnectorTableMetadata(schemaTableName, columnMetadatas);
        }

        Map<NestedColumn, ColumnHandle> getNestedColumnHandle()
        {
            return nestedColumns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                Preconditions.checkArgument(entry.getValue() >= 0 && entry.getValue() < columnMetadatas.size(), "index is not valid");
                NestedColumn nestedColumn = entry.getKey();
                return new TestingMetadata.TestingColumnHandle(nestedColumn.getName(), entry.getValue(), VarcharType.VARCHAR);
            }));
        }
    }
}
