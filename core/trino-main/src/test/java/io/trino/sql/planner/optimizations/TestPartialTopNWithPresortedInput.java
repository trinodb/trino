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

package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.TopNNode.Step.FINAL;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPartialTopNWithPresortedInput
        extends BasePlanTest
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";

    private static final SchemaTableName tableA = new SchemaTableName(TEST_SCHEMA, "table_a");
    private static final String columnNameA = "col_a";
    private static final ColumnHandle columnHandleA = new MockConnectorColumnHandle(columnNameA, VARCHAR);
    private static final String columnNameB = "col_b";

    private static final SchemaTableName nestedField = new SchemaTableName(TEST_SCHEMA, "with_nested_field");

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(MOCK_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();
        PlanTester planTester = PlanTester.create(session);
        MockConnectorFactory mockFactory = MockConnectorFactory.builder()
                .withGetTableProperties((connectorSession, handle) -> {
                    MockConnectorTableHandle tableHandle = (MockConnectorTableHandle) handle;
                    if (tableHandle.getTableName().equals(tableA)) {
                        return new ConnectorTableProperties(
                                TupleDomain.all(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new SortingProperty<>(columnHandleA, ASC_NULLS_FIRST)));
                    }
                    else if (tableHandle.getTableName().equals(nestedField)) {
                        return new ConnectorTableProperties();
                    }
                    throw new IllegalArgumentException();
                })
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(tableA)) {
                        return ImmutableList.of(
                                new ColumnMetadata(columnNameA, VARCHAR),
                                new ColumnMetadata(columnNameB, VARCHAR));
                    }
                    else if (schemaTableName.equals(nestedField)) {
                        return ImmutableList.of(
                                new ColumnMetadata("nested", RowType.from(ImmutableList.of(RowType.field("k", INTEGER)))));
                    }
                    throw new IllegalArgumentException();
                })
                .build();
        planTester.createCatalog(MOCK_CATALOG, mockFactory, ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testWithSortedTable()
    {
        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("t_col_a", ASCENDING, FIRST));
        assertDistributedPlan("SELECT col_a FROM table_a ORDER BY 1 ASC NULLS FIRST LIMIT 10", output(
                        topN(10, orderBy, FINAL,
                                exchange(LOCAL, GATHER, ImmutableList.of(),
                                        exchange(REMOTE, GATHER, ImmutableList.of(),
                                                limit(
                                                        10,
                                                        ImmutableList.of(),
                                                        true,
                                                        orderBy.stream()
                                                                .map(PlanMatchPattern.Ordering::getField)
                                                                .collect(toImmutableList()),
                                                        tableScan("table_a", ImmutableMap.of("t_col_a", "col_a"))))))));

        assertDistributedPlan("SELECT col_a FROM table_a ORDER BY 1 ASC NULLS FIRST", output(
                        exchange(REMOTE, GATHER, orderBy,
                                exchange(LOCAL, GATHER, orderBy,
                                        sort(orderBy,
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("table_a", ImmutableMap.of("t_col_a", "col_a"))))))));

        orderBy = ImmutableList.of(sort("t_col_a", ASCENDING, LAST));
        assertDistributedPlan("SELECT col_a FROM table_a ORDER BY 1 ASC NULLS LAST LIMIT 10", output(
                topN(10, orderBy, FINAL,
                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                exchange(REMOTE, GATHER, ImmutableList.of(),
                                        topN(10, orderBy, PARTIAL,
                                                exchange(LOCAL, GATHER,
                                                        topN(10, orderBy, PARTIAL,
                                                                exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                                        topN(10, orderBy, PARTIAL,
                                                                                tableScan("table_a", ImmutableMap.of("t_col_a", "col_a"))))))))))));
    }

    @Test
    public void testWithSortedWindowFunction()
    {
        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("col_b", ASCENDING, LAST));
        assertDistributedPlan("SELECT col_b, COUNT(*) OVER (ORDER BY col_b) FROM table_a ORDER BY col_b LIMIT 5", output(
                        topN(5, orderBy, FINAL,
                                exchange(LOCAL, GATHER, ImmutableList.of(),
                                        limit(
                                                5,
                                                ImmutableList.of(),
                                                true,
                                                orderBy.stream()
                                                        .map(PlanMatchPattern.Ordering::getField)
                                                        .collect(toImmutableList()),
                                                exchange(LOCAL, REPARTITION, ImmutableList.of(),
                                                        window(
                                                                p -> p.specification(
                                                                        ImmutableList.of(),
                                                                        ImmutableList.of("col_b"),
                                                                        ImmutableMap.of("col_b", ASC_NULLS_LAST)),
                                                                anyTree(
                                                                        tableScan("table_a", ImmutableMap.of("col_b", "col_b"))))))))));
    }

    @Test
    public void testWithConstantProperty()
    {
        assertDistributedPlan("SELECT * FROM (VALUES (1), (1)) AS t (id) WHERE id = 1 ORDER BY 1 LIMIT 1", output(
                topN(1, ImmutableList.of(sort("id", ASCENDING, LAST)), FINAL,
                        exchange(LOCAL, GATHER, ImmutableList.of(),
                                anyTree(
                                        values(
                                                ImmutableList.of("id"),
                                                ImmutableList.of(
                                                        ImmutableList.of(new Constant(INTEGER, 1L)),
                                                        ImmutableList.of(new Constant(INTEGER, 1L)))))))));
    }

    @Test
    public void testNestedField()
    {
        assertDistributedPlan(
                """
                        SELECT nested.k
                        FROM with_nested_field
                        WHERE nested.k = 1
                        ORDER BY nested.k
                        LIMIT 1
                        """,
                output(
                        topN(1, ImmutableList.of(sort("k", ASCENDING, LAST)), FINAL,
                                anyTree(
                                        limit(1, ImmutableList.of(), true, ImmutableList.of("k"),
                                                project(ImmutableMap.of("k", expression(new FieldReference(new Reference(RowType.from(ImmutableList.of(RowType.field("k", INTEGER))), "nested"), 0))),
                                                        filter(
                                                                new Comparison(EQUAL, new FieldReference(new Reference(RowType.from(ImmutableList.of(RowType.field("k", INTEGER))), "nested"), 0), new Constant(INTEGER, 1L)),
                                                                tableScan("with_nested_field", ImmutableMap.of("nested", "nested")))))))));
    }
}
