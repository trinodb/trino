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
package io.trino.sql.planner.iterative.rule.fuse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;

public class TestFuseCrossJoinedGlobalAggregations
        extends BaseRuleTest
{
    private static final String TEST_SCHEMA = "schema";
    private static final String TEST_TABLE_NAME = "table";
    private static final TableHandle TEST_TABLE_HANDLE = createTestTableHandle(TEST_TABLE_NAME);

    private static final MockConnectorColumnHandle TEST_TABLE_COLUMN_A = new MockConnectorColumnHandle("col_a", BIGINT);
    private static final MockConnectorColumnHandle TEST_TABLE_COLUMN_B = new MockConnectorColumnHandle("col_b", BIGINT);

    @Override
    protected Optional<LocalQueryRunner> createLocalQueryRunner()
    {
        Session defaultSession = TestingSession.testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TEST_SCHEMA)
                .build();

        LocalQueryRunner queryRunner = LocalQueryRunner.create(defaultSession);

        queryRunner.createCatalog(
                TEST_CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withGetColumns(schemaTableName -> ImmutableList.of(
                                new ColumnMetadata(TEST_TABLE_COLUMN_A.getName(), TEST_TABLE_COLUMN_A.getType()),
                                new ColumnMetadata(TEST_TABLE_COLUMN_B.getName(), TEST_TABLE_COLUMN_B.getType())))
                        .build(),
                Map.of());

        return Optional.of(queryRunner);
    }

    @Test
    public void testDoesNotFireForDifferentTables()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol col = p.symbol("col");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(p.symbol("countLeft"), expression("count(*)"), ImmutableList.of())
                                    .source(p.tableScan(createTestTableHandle("other table"), ImmutableList.of(), ImmutableMap.of()))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(p.symbol("sumRight"), expression("sum(col)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(col), ImmutableMap.of(col, TEST_TABLE_COLUMN_A)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireIfSubPlanIsMismatched()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol col = p.symbol("col");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(p.symbol("countLeft"), expression("count(*)"), ImmutableList.of())
                                    .source(p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(), ImmutableMap.of()))),
                            p.aggregation(aggregation -> aggregation
                                    .globalGrouping()
                                    .addAggregation(p.symbol("sumRight"), expression("sum(col)"), ImmutableList.of(BIGINT))
                                    .source(p.aggregation(secondAggregation -> secondAggregation
                                            .singleGroupingSet(col)
                                            .source(p.tableScan(TEST_TABLE_HANDLE, List.of(col), ImmutableMap.of(col, TEST_TABLE_COLUMN_A)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireIfSubPlanContainsUnsupportedNodes()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol col = p.symbol("col");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(p.symbol("countLeft"), expression("count(*)"), ImmutableList.of())
                                    .source(p.limit(Long.MAX_VALUE, p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(), ImmutableMap.of())))),
                            p.aggregation(aggregation -> aggregation
                                    .globalGrouping()
                                    .addAggregation(p.symbol("sumRight"), expression("sum(col)"), ImmutableList.of(BIGINT))
                                    .source((p.limit(Long.MAX_VALUE, p.tableScan(TEST_TABLE_HANDLE, List.of(col), ImmutableMap.of(col, TEST_TABLE_COLUMN_A)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDistinctAggregation()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(distinct colLeft)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A)))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonGlobalAggregation()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .singleGroupingSet(colLeft)
                                    .addAggregation(countLeft, expression("count(colLeft)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A)))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .singleGroupingSet(colRight)
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A)))));
                })
                .doesNotFire();
    }

    @Test
    public void testTableScan()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(colLeft)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A)))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A)))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("countLeft"), functionCall("count", ImmutableList.of("colLeft")),
                                        Optional.of("sumRight"), functionCall("sum", ImmutableList.of("colLeft"))),
                                Optional.empty(),
                                SINGLE,
                                tableScan(TEST_TABLE_NAME, ImmutableMap.of("colLeft", "col_a"))));
    }

    @Test
    public void testDifferentColumns()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(colLeft)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A)))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_B)))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("countLeft"), functionCall("count", ImmutableList.of("colLeft")),
                                        Optional.of("sumRight"), functionCall("sum", ImmutableList.of("colRight"))),
                                Optional.empty(),
                                SINGLE,
                                tableScan(TEST_TABLE_NAME, ImmutableMap.of("colLeft", "col_a", "colRight", "col_b"))));
    }

    @Test
    public void testTableScanWithIdentityProjection()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.project(
                                    Assignments.identity(countLeft),
                                    p.aggregation(aggregationBuilder -> aggregationBuilder
                                            .globalGrouping()
                                            .addAggregation(countLeft, expression("count(colLeft)"), ImmutableList.of(BIGINT))
                                            .source(p.project(
                                                    Assignments.identity(colLeft),
                                                    p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A)))))),
                            p.project(
                                    Assignments.identity(sumRight),
                                    p.aggregation(aggregationBuilder -> aggregationBuilder
                                            .globalGrouping()
                                            .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                            .source(p.project(
                                                    Assignments.identity(colRight),
                                                    p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A)))))));
                })
                .doesNotFire();
    }

    @Test
    public void testTableScanReusesColumnAlias()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol avgLeft = p.symbol("avgLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(avgLeft, expression("avg(colLeft)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A)))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A)))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("avgLeft"), functionCall("avg", ImmutableList.of("colLeft")),
                                        Optional.of("sumRight"), functionCall("sum", ImmutableList.of("colLeft"))),
                                Optional.empty(),
                                SINGLE,
                                tableScan(TEST_TABLE_NAME, ImmutableMap.of("colLeft", "col_a"))));
    }

    @Test
    public void testTableScanWithFilter()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(*)"), ImmutableList.of())
                                    .source(p.filter(
                                            expression("colLeft = 1"),
                                            p.tableScan(TEST_TABLE_HANDLE, List.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A))))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.filter(
                                            expression("colRight = 2"),
                                            p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A))))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("countLeft"), functionCall("count", ImmutableList.of()),
                                        Optional.of("sumRight"), functionCall("sum", ImmutableList.of("colLeft"))),
                                ImmutableList.of(),
                                ImmutableList.of("aggr_mask_left", "aggr_mask_right"),
                                Optional.empty(),
                                SINGLE,
                                project(
                                        ImmutableMap.of(
                                                "aggr_mask_left", PlanMatchPattern.expression("colLeft = 1"),
                                                "aggr_mask_right", PlanMatchPattern.expression("colLeft = 2")),
                                        filter(
                                                "colLeft = 1 OR colLeft = 2",
                                                tableScan(TEST_TABLE_NAME, ImmutableMap.of("colLeft", "col_a"))))));
    }

    @Test
    public void testTableScanWithTheSameFilter()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(*)"), ImmutableList.of())
                                    .source(p.filter(
                                            expression("colLeft = 1"),
                                            p.tableScan(TEST_TABLE_HANDLE, List.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A))))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.filter(
                                            expression("colRight = 1"),
                                            p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A))))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("countLeft"), functionCall("count", ImmutableList.of()),
                                        Optional.of("sumRight"), functionCall("sum", ImmutableList.of("colLeft"))),
                                Optional.empty(),
                                SINGLE,
                                filter(
                                        "colLeft = 1",
                                        tableScan(TEST_TABLE_NAME, ImmutableMap.of("colLeft", "col_a")))));
    }

    @Test
    public void testTableScanWithFilterOnlyOnTheRightSide()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(*)"), ImmutableList.of())
                                    .source(p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(), ImmutableMap.of()))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.filter(
                                            expression("colRight = 2"),
                                            p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A))))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("countLeft"), functionCall("count", ImmutableList.of()),
                                        Optional.of("sumRight"), functionCall("sum", ImmutableList.of("colRight"))),
                                ImmutableList.of(),
                                ImmutableList.of("aggr_mask_right"),
                                Optional.empty(),
                                SINGLE,
                                project(
                                        ImmutableMap.of(
                                                "aggr_mask_right", PlanMatchPattern.expression("colRight = 2")),
                                        tableScan(TEST_TABLE_NAME, ImmutableMap.of("colRight", "col_a")))));
    }

    @Test
    public void testTableScanWithFilterOnlyOnTheLeftSide()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(*)"), ImmutableList.of())
                                    .source(p.filter(
                                            expression("colLeft = 1"),
                                            p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A))))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(
                                            p.tableScan(TEST_TABLE_HANDLE, ImmutableList.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A)))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("countLeft"), functionCall("count", ImmutableList.of()),
                                        Optional.of("sumRight"), functionCall("sum", ImmutableList.of("colLeft"))),
                                ImmutableList.of(),
                                ImmutableList.of("aggr_mask_left"),
                                Optional.empty(),
                                SINGLE,
                                project(
                                        ImmutableMap.of(
                                                "aggr_mask_left", PlanMatchPattern.expression("colLeft = 1")),
                                        tableScan(TEST_TABLE_NAME, ImmutableMap.of("colLeft", "col_a")))));
    }

    // this fails due to test infra assumption that projection always has unique target expression
    @Test(enabled = false)
    public void testTableScanReuseAggregation()
    {
        tester().assertThat(new FuseCrossJoinedGlobalAggregations())
                .on(p -> {
                    Symbol colLeft = p.symbol("colLeft");
                    Symbol colRight = p.symbol("colRight");
                    Symbol countLeft = p.symbol("countLeft");
                    Symbol sumLeft = p.symbol("sumLeft");
                    Symbol sumRight = p.symbol("sumRight");

                    return p.join(
                            INNER,
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(countLeft, expression("count(*)"), ImmutableList.of())
                                    .addAggregation(sumLeft, expression("sum(colLeft)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colLeft), ImmutableMap.of(colLeft, TEST_TABLE_COLUMN_A)))),
                            p.aggregation(aggregationBuilder -> aggregationBuilder
                                    .globalGrouping()
                                    .addAggregation(sumRight, expression("sum(colRight)"), ImmutableList.of(BIGINT))
                                    .source(p.tableScan(TEST_TABLE_HANDLE, List.of(colRight), ImmutableMap.of(colRight, TEST_TABLE_COLUMN_A)))));
                })
                .matches(
                        project(ImmutableMap.of(
                                        "countLeft", PlanMatchPattern.expression("countLeft"),
                                        "sumLeft", PlanMatchPattern.expression("sumLeft"),
                                        "sumRight", PlanMatchPattern.expression("sumLeft")),
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(
                                                Optional.of("countLeft"), functionCall("count", ImmutableList.of()),
                                                Optional.of("sumLeft"), functionCall("sum", ImmutableList.of("colLeft"))),
                                        Optional.empty(),
                                        SINGLE,
                                        tableScan(TEST_TABLE_NAME, ImmutableMap.of("colLeft", "col_a")))));
    }

    private static TableHandle createTestTableHandle(String tableName)
    {
        return new TableHandle(
                TEST_CATALOG_HANDLE,
                new MockConnectorTableHandle(new SchemaTableName(TEST_SCHEMA, tableName)),
                new ConnectorTransactionHandle() {});
    }
}
