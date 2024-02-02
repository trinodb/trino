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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.expression.Constant;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;

public class TestPushMergeWriterUpdateIntoConnector
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);

    @Test
    public void testPushUpdateIntoConnector()
    {
        List<String> columnNames = ImmutableList.of("column_1", "column_2");
        MockConnectorFactory factory = MockConnectorFactory.builder().build();
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build()) {
            ruleTester.assertThat(createRule(ruleTester))
                    .on(p -> {
                        Symbol mergeRow = p.symbol("merge_row");
                        Symbol rowId = p.symbol("row_id");
                        Symbol rowCount = p.symbol("row_count");
                        // set column name and constant update
                        Expression updateMergeRowExpression = new Row(ImmutableList.of(p.symbol("column_1").toSymbolReference(), new LongLiteral("1"), new BooleanLiteral("true"), new LongLiteral("1"), new LongLiteral("1")));

                        return p.tableFinish(
                                p.merge(
                                        p.mergeProcessor(SCHEMA_TABLE_NAME,
                                                p.project(new Assignments(Map.of(mergeRow, updateMergeRowExpression)),
                                                        p.tableScan(tableScanBuilder -> tableScanBuilder
                                                                .setAssignments(ImmutableMap.of())
                                                                .setSymbols(ImmutableList.of())
                                                                .setTableHandle(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE)).build())),
                                                mergeRow,
                                                rowId,
                                                ImmutableList.of(),
                                                ImmutableList.of(),
                                                ImmutableList.of()),
                                        p.mergeTarget(SCHEMA_TABLE_NAME, new TableWriterNode.MergeParadigmAndTypes(Optional.of(DELETE_ROW_AND_INSERT_ROW), ImmutableList.of(), columnNames, INTEGER)),
                                        mergeRow,
                                        rowId,
                                        ImmutableList.of()),
                                p.mergeTarget(SCHEMA_TABLE_NAME),
                                rowCount);
                    })
                    .matches(node(TableUpdateNode.class));
        }
    }

    @Test
    public void testPushUpdateIntoConnectorArithmeticExpression()
    {
        List<String> columnNames = ImmutableList.of("column_1", "column_2");
        MockConnectorFactory factory = MockConnectorFactory.builder().build();
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build()) {
            ruleTester.assertThat(createRule(ruleTester))
                    .on(p -> {
                        Symbol mergeRow = p.symbol("merge_row");
                        Symbol rowId = p.symbol("row_id");
                        Symbol rowCount = p.symbol("row_count");
                        // set arithmetic expression which we don't support yet
                        Expression updateMergeRowExpression = new Row(ImmutableList.of(p.symbol("column_1").toSymbolReference(),
                                new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, p.symbol("col1").toSymbolReference(), new LongLiteral("5"))));

                        return p.tableFinish(
                                p.merge(
                                        p.mergeProcessor(SCHEMA_TABLE_NAME,
                                                p.project(new Assignments(Map.of(mergeRow, updateMergeRowExpression)),
                                                        p.tableScan(tableScanBuilder -> tableScanBuilder
                                                                .setAssignments(ImmutableMap.of())
                                                                .setSymbols(ImmutableList.of())
                                                                .setTableHandle(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE)).build())),
                                                mergeRow,
                                                rowId,
                                                ImmutableList.of(),
                                                ImmutableList.of(),
                                                ImmutableList.of()),
                                        p.mergeTarget(SCHEMA_TABLE_NAME, new TableWriterNode.MergeParadigmAndTypes(Optional.of(DELETE_ROW_AND_INSERT_ROW), ImmutableList.of(), columnNames, INTEGER)),
                                        mergeRow,
                                        rowId,
                                        ImmutableList.of()),
                                p.mergeTarget(SCHEMA_TABLE_NAME),
                                rowCount);
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testPushUpdateIntoConnectorUpdateAll()
    {
        List<String> columnNames = ImmutableList.of("column_1", "column_2");
        MockConnectorFactory factory = MockConnectorFactory.builder().build();
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build()) {
            ruleTester.assertThat(createRule(ruleTester))
                    .on(p -> {
                        Symbol mergeRow = p.symbol("merge_row");
                        Symbol rowId = p.symbol("row_id");
                        Symbol rowCount = p.symbol("row_count");
                        // set function call, which represents update all columns statement
                        Expression updateMergeRowExpression = new Row(ImmutableList.of(new FunctionCall(
                                ruleTester.getMetadata().resolveBuiltinFunction("from_base64", fromTypes(VARCHAR)).toQualifiedName(),
                                ImmutableList.of(new StringLiteral("")))));

                        return p.tableFinish(
                                p.merge(
                                        p.mergeProcessor(SCHEMA_TABLE_NAME,
                                                p.project(new Assignments(Map.of(mergeRow, updateMergeRowExpression)),
                                                        p.tableScan(tableScanBuilder -> tableScanBuilder
                                                                .setAssignments(ImmutableMap.of())
                                                                .setSymbols(ImmutableList.of())
                                                                .setTableHandle(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE)).build())),
                                                mergeRow,
                                                rowId,
                                                ImmutableList.of(),
                                                ImmutableList.of(),
                                                ImmutableList.of()),
                                        p.mergeTarget(SCHEMA_TABLE_NAME, new TableWriterNode.MergeParadigmAndTypes(Optional.of(DELETE_ROW_AND_INSERT_ROW), ImmutableList.of(), columnNames, INTEGER)),
                                        mergeRow,
                                        rowId,
                                        ImmutableList.of()),
                                p.mergeTarget(SCHEMA_TABLE_NAME),
                                rowCount);
                    })
                    .doesNotFire();
        }
    }

    private static PushMergeWriterUpdateIntoConnector createRule(RuleTester tester)
    {
        PlannerContext plannerContext = tester.getPlannerContext();
        IrTypeAnalyzer typeAnalyzer = tester.getTypeAnalyzer();
        return new PushMergeWriterUpdateIntoConnector(
                plannerContext,
                typeAnalyzer,
                new AbstractMockMetadata()
                {
                    @Override
                    public Optional<TableHandle> applyUpdate(Session session, TableHandle tableHandle, Map<ColumnHandle, Constant> assignments)
                    {
                        return Optional.of(tableHandle);
                    }

                    @Override
                    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
                    {
                        return Map.of("column_1", new TestingColumnHandle("column_1"),
                                "column_2", new TestingColumnHandle("column_2"));
                    }
                });
    }
}
