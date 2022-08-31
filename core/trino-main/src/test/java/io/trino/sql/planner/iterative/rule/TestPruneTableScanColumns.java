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
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictConstrainedTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPruneTableScanColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneTableScanColumns(tester().getMetadata()))
                .on(p -> {
                    Symbol orderdate = p.symbol("orderdate", DATE);
                    Symbol totalprice = p.symbol("totalprice", DOUBLE);
                    return p.project(
                            Assignments.of(p.symbol("x"), totalprice.toSymbolReference()),
                            p.tableScan(
                                    tester().getCurrentCatalogTableHandle(TINY_SCHEMA_NAME, "orders"),
                                    ImmutableList.of(orderdate, totalprice),
                                    ImmutableMap.of(
                                            orderdate, new TpchColumnHandle(orderdate.getName(), DATE),
                                            totalprice, new TpchColumnHandle(totalprice.getName(), DOUBLE))));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("x_", PlanMatchPattern.expression("totalprice_")),
                                strictTableScan("orders", ImmutableMap.of("totalprice_", "totalprice"))));
    }

    @Test
    public void testPruneEnforcedConstraint()
    {
        tester().assertThat(new PruneTableScanColumns(tester().getMetadata()))
                .on(p -> {
                    Symbol orderdate = p.symbol("orderdate", DATE);
                    Symbol totalprice = p.symbol("totalprice", DOUBLE);
                    TpchColumnHandle orderdateHandle = new TpchColumnHandle(orderdate.getName(), DATE);
                    TpchColumnHandle totalpriceHandle = new TpchColumnHandle(totalprice.getName(), DOUBLE);
                    return p.project(
                            Assignments.of(p.symbol("x"), totalprice.toSymbolReference()),
                            p.tableScan(
                                    tester().getCurrentCatalogTableHandle(TINY_SCHEMA_NAME, "orders"),
                                    List.of(orderdate, totalprice),
                                    Map.of(
                                            orderdate, orderdateHandle,
                                            totalprice, totalpriceHandle),
                                    TupleDomain.withColumnDomains(Map.of(
                                            orderdateHandle, Domain.notNull(DATE),
                                            totalpriceHandle, Domain.notNull(DOUBLE)))));
                })
                .matches(
                        strictProject(
                                Map.of("X", PlanMatchPattern.expression("TOTALPRICE")),
                                strictConstrainedTableScan(
                                        "orders",
                                        Map.of("TOTALPRICE", "totalprice"),
                                        // No orderdate constraint
                                        Map.of("totalprice", Domain.notNull(DOUBLE)))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneTableScanColumns(tester().getMetadata()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("y"), expression("x")),
                                p.tableScan(
                                        ImmutableList.of(p.symbol("x")),
                                        ImmutableMap.of(p.symbol("x"), new TestingColumnHandle("x")))))
                .doesNotFire();
    }

    @Test
    public void testPushColumnPruningProjection()
    {
        String testSchema = "test_schema";
        String testTable = "test_table";
        SchemaTableName testSchemaTable = new SchemaTableName(testSchema, testTable);
        ColumnHandle columnHandleA = new MockConnectorColumnHandle("cola", DATE);
        ColumnHandle columnHandleB = new MockConnectorColumnHandle("colb", DOUBLE);
        Map<String, ColumnHandle> assignments = ImmutableMap.of(
                "cola", columnHandleA,
                "colb", columnHandleB);

        // Create catalog with applyProjection
        MockConnectorFactory factory = MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of(testSchema))
                .withListTables((connectorSession, schema) -> testSchema.equals(schema) ? ImmutableList.of(testTable) : ImmutableList.of())
                .withGetColumns(schemaTableName -> assignments.entrySet().stream()
                        .map(entry -> new ColumnMetadata(entry.getKey(), ((MockConnectorColumnHandle) entry.getValue()).getType()))
                        .collect(toImmutableList()))
                .withApplyProjection(this::mockApplyProjection)
                .build();
        try (RuleTester ruleTester = RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build()) {
            ruleTester.assertThat(new PruneTableScanColumns(ruleTester.getMetadata()))
                    .on(p -> {
                        Symbol symbolA = p.symbol("cola", DATE);
                        Symbol symbolB = p.symbol("colb", DOUBLE);
                        return p.project(
                                Assignments.of(p.symbol("x"), symbolB.toSymbolReference()),
                                p.tableScan(
                                        ruleTester.getCurrentCatalogTableHandle(testSchema, testTable),
                                        ImmutableList.of(symbolA, symbolB),
                                        ImmutableMap.of(
                                                symbolA, columnHandleA,
                                                symbolB, columnHandleB)));
                    })
                    .withSession(testSessionBuilder().setCatalog(TEST_CATALOG_NAME).setSchema(testSchema).build())
                    .matches(
                            strictProject(
                                    ImmutableMap.of("expr", PlanMatchPattern.expression("COLB")),
                                    tableScan(
                                            new MockConnectorTableHandle(
                                                    testSchemaTable,
                                                    TupleDomain.all(),
                                                    Optional.of(ImmutableList.of(columnHandleB)))::equals,
                                            TupleDomain.all(),
                                            ImmutableMap.of("COLB", columnHandleB::equals))));
        }
    }

    private Optional<ProjectionApplicationResult<ConnectorTableHandle>> mockApplyProjection(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        MockConnectorTableHandle handle = (MockConnectorTableHandle) tableHandle;

        List<Variable> variables = projections.stream()
                .map(Variable.class::cast)
                .collect(toImmutableList());
        List<ColumnHandle> newColumns = variables.stream()
                .map(variable -> assignments.get(variable.getName()))
                .collect(toImmutableList());
        if (handle.getColumns().isPresent() && newColumns.equals(handle.getColumns().get())) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectionApplicationResult<>(
                        new MockConnectorTableHandle(handle.getTableName(), handle.getConstraint(), Optional.of(newColumns)),
                        projections,
                        variables.stream()
                                .map(variable -> new Assignment(
                                        variable.getName(),
                                        assignments.get(variable.getName()),
                                        ((MockConnectorColumnHandle) assignments.get(variable.getName())).getType()))
                                .collect(toImmutableList()),
                        false));
    }
}
