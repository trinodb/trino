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
package io.trino.cache;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingHandles;
import io.trino.testing.TestingMetadata;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.cache.CanonicalSubplanExtractor.extractCanonicalSubplans;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCanonicalSubplanExtractor
        extends BasePlanTest
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private static final CacheTableId CACHE_TABLE_ID = new CacheTableId("cache_table_id");
    private static final PlanNodeId SCAN_NODE_ID = new PlanNodeId("scan_id");
    private static final String CATALOG_ID = TEST_TABLE_HANDLE.getCatalogHandle().getId();
    private static final CacheTableId CATALOG_CACHE_TABLE_ID = new CacheTableId(CATALOG_ID + ":" + CACHE_TABLE_ID);
    private static final Metadata TEST_METADATA = new AbstractMockMetadata() {};
    private static final CacheMetadata TEST_CACHE_METADATA = new TestCacheMetadata();

    private PlanBuilder planBuilder;
    private String tpchCatalogId;

    public TestCanonicalSubplanExtractor()
    {
        super(ImmutableMap.of(
                // increase task concurrency to get parallel plans
                TASK_CONCURRENCY, "4"));
    }

    @BeforeAll
    public void setup()
    {
        planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
        tpchCatalogId = getQueryRunner().getCatalogHandle(getQueryRunner().getDefaultSession().getCatalog().orElseThrow()).getId();
    }

    @Test
    public void testAggregationWithMultipleGroupByColumnsAndPredicate()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey), sum(nationkey) filter(where nationkey > 10)
                FROM nation
                GROUP BY name, regionkey * 2
                HAVING name = '0123456789012345689012345' AND sum(nationkey) > BIGINT '10'""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();
        assertThat(nonAggregatedSubplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[nationkey:bigint]"), new SymbolReference("[nationkey:bigint]")),
                new SimpleEntry<>(new CacheColumnId("[name:varchar(25)]"), new SymbolReference("[name:varchar(25)]")),
                new SimpleEntry<>(new CacheColumnId("((\"[nationkey:bigint]\" > BIGINT '10'))"), expression("(\"[nationkey:bigint]\" > BIGINT '10')")),
                new SimpleEntry<>(new CacheColumnId("((\"[regionkey:bigint]\" * BIGINT '2'))"), expression("(\"[regionkey:bigint]\" * BIGINT '2')")));

        Expression sum = getSumFunction();
        Expression filteredSum = getSumFunctionBuilder()
                .setFilter(expression("(\"[nationkey:bigint]\" > BIGINT '10')"))
                .build();
        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(
                expression("\"[name:varchar(25)]\""),
                expression("(\"[regionkey:bigint]\" * BIGINT '2')")));
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                new CacheColumnId("[nationkey:bigint]"),
                new CacheColumnId("[name:varchar(25)]"),
                new CacheColumnId("[regionkey:bigint]"),
                new CacheColumnId("((\"[nationkey:bigint]\" > BIGINT '10'))"),
                new CacheColumnId("((\"[regionkey:bigint]\" * BIGINT '2'))"),
                new CacheColumnId("(" + formatExpression(filteredSum) + ")"),
                new CacheColumnId("(" + formatExpression(sum) + ")"));
        assertThat(aggregatedSubplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[name:varchar(25)]"), expression("\"[name:varchar(25)]\"")),
                new SimpleEntry<>(new CacheColumnId("((\"[regionkey:bigint]\" * BIGINT '2'))"), expression("(\"[regionkey:bigint]\" * BIGINT '2')")),
                new SimpleEntry<>(new CacheColumnId("(" + formatExpression(filteredSum) + ")"), filteredSum),
                new SimpleEntry<>(new CacheColumnId("(" + formatExpression(sum) + ")"), sum));
        assertThat(aggregatedSubplan.getConjuncts()).containsExactly(expression("(\"[name:varchar(25)]\" = '0123456789012345689012345')"));
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[nationkey:bigint]"), new TpchColumnHandle("nationkey", BIGINT)),
                new SimpleEntry<>(new CacheColumnId("[name:varchar(25)]"), new TpchColumnHandle("name", createVarcharType(25))),
                new SimpleEntry<>(new CacheColumnId("[regionkey:bigint]"), new TpchColumnHandle("regionkey", BIGINT)));
        assertThat(aggregatedSubplan.getTableId()).isEqualTo(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"));
    }

    @Test
    public void testAggregationWithMultipleGroupByColumns()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey + 1)
                FROM nation
                GROUP BY name, regionkey""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();

        assertThat(nonAggregatedSubplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[name:varchar(25)]"), new SymbolReference("[name:varchar(25)]")),
                new SimpleEntry<>(new CacheColumnId("[regionkey:bigint]"), new SymbolReference("[regionkey:bigint]")),
                new SimpleEntry<>(new CacheColumnId("((\"[nationkey:bigint]\" + BIGINT '1'))"), expression("(\"[nationkey:bigint]\" + BIGINT '1')")));

        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(
                expression("\"[name:varchar(25)]\""),
                expression("\"[regionkey:bigint]\"")));

        Expression sum = getFunctionCallBuilder("sum", new ExpressionWithType("(\"[nationkey:bigint]\" + BIGINT '1')", BIGINT)).build();
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                new CacheColumnId("[nationkey:bigint]"),
                new CacheColumnId("[name:varchar(25)]"),
                new CacheColumnId("[regionkey:bigint]"),
                new CacheColumnId("((\"[nationkey:bigint]\" + BIGINT '1'))"),
                new CacheColumnId("(" + formatExpression(sum) + ")"));
        assertThat(aggregatedSubplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[name:varchar(25)]"), expression("\"[name:varchar(25)]\"")),
                new SimpleEntry<>(new CacheColumnId("[regionkey:bigint]"), expression("\"[regionkey:bigint]\"")),
                new SimpleEntry<>(new CacheColumnId("(" + formatExpression(sum) + ")"), sum));
        assertThat(aggregatedSubplan.getConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[nationkey:bigint]"), new TpchColumnHandle("nationkey", BIGINT)),
                new SimpleEntry<>(new CacheColumnId("[name:varchar(25)]"), new TpchColumnHandle("name", createVarcharType(25))),
                new SimpleEntry<>(new CacheColumnId("[regionkey:bigint]"), new TpchColumnHandle("regionkey", BIGINT)));
        assertThat(aggregatedSubplan.getTableId()).isEqualTo(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"));
    }

    @Test
    public void testBigintAggregation()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey)
                FROM nation
                GROUP BY regionkey""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();
        assertThat(nonAggregatedSubplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[nationkey:bigint]"), new SymbolReference("[nationkey:bigint]")),
                new SimpleEntry<>(new CacheColumnId("[regionkey:bigint]"), new SymbolReference("[regionkey:bigint]")));

        Expression sum = getSumFunction();
        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(expression("\"[regionkey:bigint]\"")));
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                new CacheColumnId("[nationkey:bigint]"),
                new CacheColumnId("[regionkey:bigint]"),
                new CacheColumnId("(" + formatExpression(sum) + ")"));
        assertThat(aggregatedSubplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[regionkey:bigint]"), new SymbolReference("[regionkey:bigint]")),
                new SimpleEntry<>(new CacheColumnId("(" + formatExpression(sum) + ")"), sum));
        assertThat(aggregatedSubplan.getConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
    }

    @Test
    public void testGlobalAggregation()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey)
                FROM nation""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();

        Expression sum = getSumFunction();
        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(aggregatedSubplan.getGroupByColumns()).contains(ImmutableSet.of());
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                new CacheColumnId("[nationkey:bigint]"),
                new CacheColumnId("(" + formatExpression(sum) + ")"));
        assertThat(aggregatedSubplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("(" + formatExpression(sum) + ")"), sum));
        assertThat(aggregatedSubplan.getConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
    }

    @Test
    public void testNestedAggregations()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(sum_nationkey)
                FROM (SELECT sum(nationkey) sum_nationkey, name
                      FROM nation
                      GROUP BY name, regionkey)
                GROUP BY name || 'abc'""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();

        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(
                expression("\"[name:varchar(25)]\""),
                expression("\"[regionkey:bigint]\"")));
    }

    @Test
    public void testUnsupportedAggregations()
    {
        assertUnsupportedAggregation("SELECT array_agg(nationkey order by nationkey) FROM nation");
        assertUnsupportedAggregation("SELECT sum(nationkey) FROM nation GROUP BY ROLLUP (nationkey)");
        assertUnsupportedAggregation("SELECT sum(distinct nationkey), sum(distinct regionkey) FROM nation");
    }

    private void assertUnsupportedAggregation(@Language("SQL") String query)
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery(query);
        assertThat(subplans).hasSize(1);
        assertThat(getOnlyElement(subplans).getGroupByColumns()).isEmpty();
    }

    private Optional<List<Expression>> getGroupByExpressions(CanonicalSubplan subplan)
    {
        return subplan.getGroupByColumns()
                .map(columns -> columns.stream()
                        .map(column -> requireNonNull(subplan.getAssignments().get(column), "No assignment for column: " + column))
                        .collect(toImmutableList()));
    }

    @Test
    public void testExtractCanonicalScanAndProject()
    {
        ProjectNode projectNode = createScanAndProjectNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_CACHE_METADATA,
                TEST_SESSION,
                projectNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(projectNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new Symbol("symbol2")),
                new SimpleEntry<>(new CacheColumnId("((\"[cache_column1]\" + 1))"), new Symbol("projection1")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("((\"[cache_column1]\" + 1))"), expression("\"[cache_column1]\" + 1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new SymbolReference("[cache_column2]")));

        assertThat(subplan.getConjuncts()).isEmpty();
        assertThat(subplan.getDynamicConjuncts()).isEmpty();
        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testExtractCanonicalFilterAndProject()
    {
        ProjectNode projectNode = createFilterAndProjectNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_CACHE_METADATA,
                TEST_SESSION,
                projectNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(projectNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new Symbol("symbol2")),
                new SimpleEntry<>(new CacheColumnId("((\"[cache_column1]\" + 1))"), new Symbol("projection1")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("((\"[cache_column1]\" + 1))"), expression("\"[cache_column1]\" + 1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new SymbolReference("[cache_column2]")));

        assertThat(subplan.getConjuncts()).hasSize(1);
        Expression predicate = getOnlyElement(subplan.getConjuncts());
        assertThat(predicate).isEqualTo(expression("\"[cache_column1]\" + \"[cache_column2]\" > 0"));

        assertThat(subplan.getDynamicConjuncts()).hasSize(1);
        Expression dynamicFilterExpression = getOnlyElement(subplan.getDynamicConjuncts());
        assertThat(DynamicFilters.getDescriptor(dynamicFilterExpression)).contains(
                new DynamicFilters.Descriptor(new DynamicFilterId("dynamic_filter_id"), expression("\"[cache_column1]\"")));

        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testExtractCanonicalFilter()
    {
        FilterNode filterNode = createFilterNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_CACHE_METADATA,
                TEST_SESSION,
                filterNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(filterNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new Symbol("symbol2")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new SymbolReference("[cache_column1]")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new SymbolReference("[cache_column2]")));

        assertThat(subplan.getConjuncts()).hasSize(1);
        Expression predicate = getOnlyElement(subplan.getConjuncts());
        assertThat(predicate).isEqualTo(expression("\"[cache_column1]\" + \"[cache_column2]\" > 0"));

        assertThat(subplan.getDynamicConjuncts()).hasSize(1);
        Expression dynamicFilterExpression = getOnlyElement(subplan.getDynamicConjuncts());
        assertThat(DynamicFilters.getDescriptor(dynamicFilterExpression)).contains(
                new DynamicFilters.Descriptor(new DynamicFilterId("dynamic_filter_id"), expression("\"[cache_column1]\"")));

        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testExtractCanonicalTableScan()
    {
        // no cache id, therefore no canonical plan
        TableScanNode tableScanNode = createTableScan();
        assertThat(extractCanonicalSubplans(
                TEST_METADATA,
                new TestCacheMetadata(Optional.empty(), handle -> Optional.of(new CacheColumnId(handle.getName()))),
                TEST_SESSION,
                tableScanNode))
                .isEmpty();

        // no column id, therefore no canonical plan
        assertThat(extractCanonicalSubplans(
                TEST_METADATA,
                new TestCacheMetadata(Optional.of(CACHE_TABLE_ID), handle -> Optional.empty()),
                TEST_SESSION,
                tableScanNode))
                .isEmpty();

        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_CACHE_METADATA,
                TEST_SESSION,
                tableScanNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(tableScanNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new Symbol("symbol2")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new SymbolReference("[cache_column1]")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new SymbolReference("[cache_column2]")));
        assertThat(subplan.getConjuncts()).isEmpty();
        assertThat(subplan.getDynamicConjuncts()).isEmpty();
        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("[cache_column1]"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("[cache_column2]"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testProjectionWithDuplicatedExpressions()
    {
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol("alias1"),
                        expression("symbol1 * 2"),
                        new Symbol("alias2"),
                        expression("symbol1 * 2"))));
    }

    @Test
    public void testAliasingProjection()
    {
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol("alias"),
                        expression("symbol1"))));
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol("symbol1"),
                        expression("symbol1"),
                        new Symbol("alias"),
                        expression("symbol1"))));
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol("alias"),
                        expression("symbol1"),
                        new Symbol("symbol1"),
                        expression("symbol1"))));
    }

    private void assertThatCanonicalSubplanIsForTableScan(PlanNode root)
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(TEST_METADATA, TEST_CACHE_METADATA, TEST_SESSION, root);
        assertThat(subplans).hasSize(1);
        assertThat(getOnlyElement(subplans).getOriginalPlanNode()).isInstanceOf(TableScanNode.class);
    }

    @Test
    public void testTableScanWithDuplicatedColumnHandle()
    {
        Symbol symbol1 = new Symbol("symbol1");
        Symbol symbol2 = new Symbol("symbol2");
        TestingColumnHandle columnHandle = new TestingColumnHandle("column1");
        TableScanNode tableScanNode = new TableScanNode(
                SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol1, symbol2),
                ImmutableMap.of(symbol2, columnHandle, symbol1, columnHandle),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        assertThat(extractCanonicalSubplans(TEST_METADATA, TEST_CACHE_METADATA, TEST_SESSION, tableScanNode)).isEmpty();
    }

    @Test
    public void testTableHandlesCanonization()
    {
        TableHandle tableHandle1 = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "table1"));
        TableHandle tableHandle2 = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "table2"));

        PlanNode root = planBuilder.union(ImmutableListMultimap.of(), ImmutableList.of(
                planBuilder.tableScan(tableHandle1, ImmutableList.of(), ImmutableMap.of(), Optional.of(false)),
                planBuilder.tableScan(tableHandle2, ImmutableList.of(), ImmutableMap.of(), Optional.of(false))));

        // TableHandles will be turned into common canonical version
        TableHandle canonicalTableHandle = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common"));
        List<CanonicalSubplan> canonicalSubplans = extractCanonicalSubplans(
                TEST_METADATA,
                new TestCacheMetadata(
                        handle -> Optional.of(new CacheColumnId(handle.getName())),
                        (tableHandle) -> canonicalTableHandle,
                        (tableHandle) -> Optional.of(new CacheTableId(tableHandle.getConnectorHandle().toString()))),
                TEST_SESSION,
                root);
        List<CacheTableId> tableIds = canonicalSubplans.stream().map(CanonicalSubplan::getTableId).toList();
        CacheTableId schemaCommonId = new CacheTableId(CATALOG_ID + ":schema.common");
        assertThat(tableIds).isEqualTo(ImmutableList.of(schemaCommonId, schemaCommonId));
        assertThat(canonicalSubplans).allMatch(subplan -> subplan.getTable().equals(canonicalTableHandle));

        // TableHandles will not be turned into common canonical version
        tableIds = extractCanonicalSubplans(
                TEST_METADATA,
                new TestCacheMetadata(
                        handle -> Optional.of(new CacheColumnId(handle.getName())),
                        (tableHandle) -> {
                            TestingMetadata.TestingTableHandle handle = (TestingMetadata.TestingTableHandle) tableHandle.getConnectorHandle();
                            if (handle.getTableName().getTableName().equals("table1")) {
                                return TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common1"));
                            }
                            else {
                                return TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common2"));
                            }
                        },
                        (tableHandle) -> Optional.of(new CacheTableId(tableHandle.getConnectorHandle().toString()))),
                TEST_SESSION,
                root).stream().map(CanonicalSubplan::getTableId).toList();
        assertThat(tableIds).isEqualTo(ImmutableList.of(
                new CacheTableId(CATALOG_ID + ":schema.common1"),
                new CacheTableId(CATALOG_ID + ":schema.common2")));
    }

    private List<CanonicalSubplan> extractCanonicalSubplansForQuery(@Language("SQL") String query)
    {
        Plan plan = plan(query);
        LocalQueryRunner queryRunner = getQueryRunner();
        return queryRunner.inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> queryRunner.getMetadata().getCatalogHandle(session, catalog));
            return extractCanonicalSubplans(queryRunner.getMetadata(), queryRunner.getCacheMetadata(), session, plan.getRoot());
        });
    }

    private Expression getSumFunction()
    {
        return getSumFunctionBuilder().build();
    }

    private BuiltinFunctionCallBuilder getSumFunctionBuilder()
    {
        return getFunctionCallBuilder("sum", new ExpressionWithType("\"[nationkey:bigint]\"", BIGINT));
    }

    private BuiltinFunctionCallBuilder getFunctionCallBuilder(String name, ExpressionWithType... arguments)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        BuiltinFunctionCallBuilder builder = BuiltinFunctionCallBuilder.resolve(queryRunner.getMetadata())
                .setName(name);
        for (ExpressionWithType argument : arguments) {
            builder.addArgument(argument.type, argument.expression);
        }
        return builder;
    }

    // workaround for https://github.com/google/error-prone/issues/2713
    @SuppressWarnings("unused")
    private record ExpressionWithType(Expression expression, Type type)
    {
        public ExpressionWithType(@Language("SQL") String expression, Type type)
        {
            this(PlanBuilder.expression(expression), type);
        }
    }

    private ProjectNode createScanAndProjectNode()
    {
        return new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol("projection1"),
                        expression("symbol1 + 1"),
                        new Symbol("symbol2"),
                        expression("symbol2")));
    }

    private ProjectNode createFilterAndProjectNode()
    {
        return new ProjectNode(
                new PlanNodeId("project_node"),
                createFilterNode(),
                Assignments.of(
                        new Symbol("projection1"),
                        expression("symbol1 + 1"),
                        new Symbol("symbol2"),
                        expression("symbol2")));
    }

    private FilterNode createFilterNode()
    {
        MetadataManager metadataManager = createTestMetadataManager();
        return new FilterNode(
                new PlanNodeId("filter_node"),
                createTableScan(),
                and(
                        expression("symbol1 + symbol2 > 0"),
                        createDynamicFilterExpression(
                                metadataManager,
                                new DynamicFilterId("dynamic_filter_id"),
                                BIGINT,
                                expression("symbol1"))));
    }

    private TableScanNode createTableScan()
    {
        Symbol symbol1 = new Symbol("symbol1");
        Symbol symbol2 = new Symbol("symbol2");
        TestingColumnHandle handle1 = new TestingColumnHandle("column1");
        TestingColumnHandle handle2 = new TestingColumnHandle("column2");
        return new TableScanNode(
                SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol1, symbol2),
                ImmutableMap.of(symbol2, handle2, symbol1, handle1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
    }

    private static class TestCacheMetadata
            extends CacheMetadata
    {
        private final Function<TableHandle, Optional<CacheTableId>> tableHandleCacheTableIdMapper;

        private final Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper;
        private final Function<TableHandle, TableHandle> canonicalizeTableHande;

        private TestCacheMetadata()
        {
            this(handle -> Optional.of(new CacheColumnId("cache_" + handle.getName())), Functions.identity(), (any) -> Optional.of(CACHE_TABLE_ID));
        }

        private TestCacheMetadata(
                Optional<CacheTableId> cacheTableId,
                Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper)
        {
            this(cacheColumnIdMapper, Function.identity(), (any) -> cacheTableId);
        }

        private TestCacheMetadata(
                Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper,
                Function<TableHandle, TableHandle> canonicalizeTableHande,
                Function<TableHandle, Optional<CacheTableId>> tableHandleCacheTableIdMapper)
        {
            super(catalogHandle -> Optional.empty());
            this.cacheColumnIdMapper = cacheColumnIdMapper;
            this.canonicalizeTableHande = canonicalizeTableHande;
            this.tableHandleCacheTableIdMapper = tableHandleCacheTableIdMapper;
        }

        @Override
        public Optional<CacheTableId> getCacheTableId(Session session, TableHandle tableHandle)
        {
            return tableHandleCacheTableIdMapper.apply(tableHandle);
        }

        @Override
        public Optional<CacheColumnId> getCacheColumnId(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return cacheColumnIdMapper.apply((TestingColumnHandle) columnHandle);
        }

        @Override
        public TableHandle getCanonicalTableHandle(Session session, TableHandle tableHandle)
        {
            return canonicalizeTableHande.apply(tableHandle);
        }
    }
}
