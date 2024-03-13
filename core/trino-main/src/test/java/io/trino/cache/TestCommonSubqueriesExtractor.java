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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters.Descriptor;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.CanonicalAggregation;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.LongLiteral;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.tree.SortItem.NullOrdering;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingTransactionHandle;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_COMMON_SUBQUERIES_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_PROJECTIONS_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalExpressionToColumnId;
import static io.trino.cache.CanonicalSubplanExtractor.columnIdToSymbol;
import static io.trino.cache.CommonSubqueriesExtractor.aggregationKey;
import static io.trino.cache.CommonSubqueriesExtractor.combine;
import static io.trino.cache.CommonSubqueriesExtractor.filterProjectKey;
import static io.trino.cache.CommonSubqueriesExtractor.scanFilterProjectKey;
import static io.trino.cache.CommonSubqueriesExtractor.topNKey;
import static io.trino.cache.CommonSubqueriesExtractor.topNRankingKey;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.extractDisjuncts;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.identityProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCommonSubqueriesExtractor
        extends BasePlanTest
{
    private static final CacheTableId CACHE_TABLE_ID = new CacheTableId("cache_table_id");
    private static final CacheColumnId REGIONKEY_ID = new CacheColumnId("[regionkey:bigint]");
    private static final CacheColumnId NATIONKEY_ID = new CacheColumnId("[nationkey:bigint]");
    private static final CacheColumnId NAME_ID = new CacheColumnId("[name:varchar(25)]");
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TEST_SCHEMA)
            .setSystemProperty(CACHE_COMMON_SUBQUERIES_ENABLED, "true")
            .build();
    private static final Session TPCH_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            // prevent CBO from interfering with tests
            .setSystemProperty(JOIN_REORDERING_STRATEGY, "none")
            // simplify tests by disabling small DF waiting
            .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT, "0")
            .build();
    private static final MockConnectorColumnHandle HANDLE_1 = new MockConnectorColumnHandle("column1", BIGINT);
    private static final MockConnectorColumnHandle HANDLE_2 = new MockConnectorColumnHandle("column2", BIGINT);
    private static final TupleDomain<ColumnHandle> CONSTRAINT_1 = TupleDomain.withColumnDomains(ImmutableMap.of(
            HANDLE_1,
            Domain.create(ValueSet.ofRanges(
                    Range.lessThan(BIGINT, 50L),
                    Range.greaterThan(BIGINT, 150L)), false)));
    private static final TupleDomain<ColumnHandle> CONSTRAINT_2 = TupleDomain.withColumnDomains(ImmutableMap.of(
            HANDLE_1,
            Domain.create(ValueSet.ofRanges(
                    Range.lessThan(BIGINT, 20L),
                    Range.greaterThan(BIGINT, 40L)), false)));

    private static final TupleDomain<ColumnHandle> CONSTRAINT_3 = TupleDomain.withColumnDomains(ImmutableMap.of(
            HANDLE_1,
            Domain.create(ValueSet.ofRanges(
                    Range.lessThan(BIGINT, 30L),
                    Range.greaterThan(BIGINT, 70L)), false)));
    private static final SchemaTableName TABLE_NAME = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
    private static final ExpressionWithType NATIONKEY_EXPRESSION = new ExpressionWithType(new SymbolReference("[nationkey:bigint]"), BIGINT);

    private TableHandle testTableHandle;
    private String tpchCatalogId;

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(TEST_SESSION);
        planTester.createCatalog(
                TEST_CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withGetColumns(handle -> ImmutableList.of(
                                new ColumnMetadata("column1", BIGINT),
                                new ColumnMetadata("column2", BIGINT)))
                        .withGetCacheTableId(handle -> Optional.of(CACHE_TABLE_ID))
                        .withGetCanonicalTableHandle(Function.identity())
                        .withGetCacheColumnId(handle -> {
                            MockConnectorColumnHandle column = (MockConnectorColumnHandle) handle;
                            return Optional.of(new CacheColumnId("cache_" + column.getName()));
                        })
                        .withApplyFilter((session, tableHandle, constraint) -> {
                            // predicate is fully subsumed
                            if (constraint.getSummary().equals(CONSTRAINT_1)) {
                                return Optional.of(new ConstraintApplicationResult<>(new MockConnectorTableHandle(TABLE_NAME, CONSTRAINT_1, Optional.of(ImmutableList.of(HANDLE_1))), TupleDomain.all(), false));
                            }
                            // predicate is rejected
                            else if (constraint.getSummary().equals(CONSTRAINT_2)) {
                                return Optional.of(new ConstraintApplicationResult<>(new MockConnectorTableHandle(TABLE_NAME, TupleDomain.all(), Optional.empty()), CONSTRAINT_2, false));
                            }
                            // predicate is subsumed opportunistically
                            else if (constraint.getSummary().equals(CONSTRAINT_3)) {
                                return Optional.of(new ConstraintApplicationResult<>(new MockConnectorTableHandle(TABLE_NAME, CONSTRAINT_3, Optional.empty()), CONSTRAINT_3, false));
                            }
                            return Optional.empty();
                        })
                        .withGetTableProperties((session, tableHandle) -> {
                            MockConnectorTableHandle handle = (MockConnectorTableHandle) tableHandle;
                            if (handle.getConstraint().equals(CONSTRAINT_2)) {
                                return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), emptyList());
                            }
                            return new ConnectorTableProperties(TupleDomain.all(), Optional.empty(), Optional.empty(), emptyList());
                        })
                        .build(),
                ImmutableMap.of());
        planTester.createCatalog(TPCH_SESSION.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        testTableHandle = new TableHandle(
                planTester.getCatalogHandle(TEST_CATALOG_NAME),
                new MockConnectorTableHandle(TABLE_NAME),
                TestingTransactionHandle.create());
        tpchCatalogId = planTester.getCatalogHandle(TPCH_SESSION.getCatalog().get()).getId();
        return planTester;
    }

    @Test
    public void testCommonDynamicFilters()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT nationkey FROM
                  ((SELECT nationkey, regionkey FROM nation n JOIN (SELECT * FROM (VALUES 0, 1) t(a)) t ON n.nationkey = t.a)
                   UNION ALL
                   (SELECT nationkey, regionkey FROM nation n JOIN (SELECT * FROM (VALUES 0, 1) t(a)) t ON n.regionkey = t.a)) l(nationkey, regionkey)
                JOIN (SELECT * FROM (VALUES 0, 1, 2) t(a)) t ON l.nationkey = t.a""");

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(FilterNode.class));

        CommonPlanAdaptation projectionA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation projectionB = Iterables.get(planAdaptations.values(), 1);

        // extract dynamic filter ids
        List<DynamicFilterId> dynamicFilterIds = PlanNodeSearcher.searchFrom(commonSubqueries.plan())
                .whereIsInstanceOfAny(JoinNode.class)
                .<JoinNode>findAll().stream()
                .flatMap(join -> join.getDynamicFilters().keySet().stream())
                .collect(toImmutableList());
        DynamicFilterId topId = dynamicFilterIds.get(0);
        DynamicFilterId leftId = dynamicFilterIds.get(1);
        DynamicFilterId rightId = dynamicFilterIds.get(2);

        List<String> symbols = commonSubqueries.planAdaptations.values().stream()
                .map(subplan -> subplan.getCommonSubplanFilteredTableScan().tableScanNode())
                .flatMap(scan -> scan.getOutputSymbols().stream())
                .map(Symbol::toString)
                .collect(toImmutableList());
        String leftNationkey = symbols.get(0);
        String leftRegionkey = symbols.get(1);
        String rightNationkey = symbols.get(2);
        String rightRegionkey = symbols.get(3);

        // assert that common subplan have dynamic filter preserved in both FilterNode and FilteredTableScan
        assertThat(extractExpressions(projectionA.getCommonSubplan()).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .collect(toImmutableList()))
                .containsExactly(
                        new Descriptor(topId, new SymbolReference(leftNationkey)),
                        new Descriptor(leftId, new SymbolReference(leftNationkey)));
        assertThat(projectionA.getCommonSubplanFilteredTableScan().filterPredicate().stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .collect(toImmutableList()))
                .containsExactly(
                        new Descriptor(topId, new SymbolReference(leftNationkey)),
                        new Descriptor(leftId, new SymbolReference(leftNationkey)));

        assertThat(extractExpressions(projectionB.getCommonSubplan()).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .collect(toImmutableList()))
                .containsExactly(
                        new Descriptor(topId, new SymbolReference(rightNationkey)),
                        new Descriptor(rightId, new SymbolReference(rightRegionkey)));
        assertThat(projectionB.getCommonSubplanFilteredTableScan().filterPredicate().stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .collect(toImmutableList()))
                .containsExactly(
                        new Descriptor(topId, new SymbolReference(rightNationkey)),
                        new Descriptor(rightId, new SymbolReference(rightRegionkey)));

        // assert that common dynamic filter is extracted for both subplans
        assertThat(extractDisjuncts(projectionA.getCommonDynamicFilterDisjuncts()).stream()
                .map(expression -> extractDynamicFilters(expression).getDynamicConjuncts()))
                .containsExactly(
                        ImmutableList.of(new Descriptor(topId, new SymbolReference(leftNationkey)), new Descriptor(leftId, new SymbolReference(leftNationkey))),
                        ImmutableList.of(new Descriptor(topId, new SymbolReference(leftNationkey)), new Descriptor(rightId, new SymbolReference(leftRegionkey))));

        assertThat(extractDisjuncts(projectionB.getCommonDynamicFilterDisjuncts()).stream()
                .map(expression -> extractDynamicFilters(expression).getDynamicConjuncts()))
                .containsExactly(
                        ImmutableList.of(new Descriptor(topId, new SymbolReference(rightNationkey)), new Descriptor(leftId, new SymbolReference(rightNationkey))),
                        ImmutableList.of(new Descriptor(topId, new SymbolReference(rightNationkey)), new Descriptor(rightId, new SymbolReference(rightRegionkey))));

        // verify DF mappings for common dynamic filter
        TpchColumnHandle nationkeyHandle = new TpchColumnHandle("nationkey", BIGINT);
        TpchColumnHandle regionkeyHandle = new TpchColumnHandle("regionkey", BIGINT);
        assertThat(projectionA.getCommonColumnHandles())
                .containsExactly(new SimpleEntry<>(NATIONKEY_ID, nationkeyHandle), new SimpleEntry<>(REGIONKEY_ID, regionkeyHandle));
        assertThat(projectionA.getCommonColumnHandles()).isEqualTo(projectionB.getCommonColumnHandles());
    }

    @Test
    public void testCacheTopNRankingRank()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        SELECT name, regionkey FROM nation WHERE nationkey > 10 ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES
                        """,
                false, true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(TopNRankingNode.class));
        CommonPlanAdaptation topNRanking = planAdaptations.values().stream().findFirst().get();
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();

        PlanMatchPattern commonSubplan = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of(),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", ASC_NULLS_LAST))
                        .rankingType(RANK)
                        .maxRankingPerPartition(6)
                        .partial(true),
                strictProject(ImmutableMap.of(
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY"))),
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "10")),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")))));
        assertTpchPlan(symbolAllocator, topNRanking.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        // validate no adaptation is required
        assertThat(topNRanking.adaptCommonSubplan(topNRanking.getCommonSubplan(), idAllocator)).isEqualTo(topNRanking.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(createVarcharType(25), BIGINT);
        assertThat(topNRanking.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNRankingKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableList.of(),
                                ImmutableMap.of(REGIONKEY_ID, ASC_NULLS_LAST),
                                RANK, 6),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        NATIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testCacheTopNRankingRankWithPullableConjuncts()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        (SELECT name, regionkey FROM nation WHERE nationkey > 10 ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES)
                        UNION ALL
                        (SELECT name, regionkey FROM nation WHERE nationkey > 10 ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES)
                        """,
                false, true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(TopNRankingNode.class));
        CommonPlanAdaptation topNA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation topNB = Iterables.get(planAdaptations.values(), 1);
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();

        PlanMatchPattern commonSubplan = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of(),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", ASC_NULLS_LAST))
                        .rankingType(RANK)
                        .maxRankingPerPartition(6)
                        .partial(true),
                strictProject(ImmutableMap.of(
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY"))),
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "10")),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")))));
        assertTpchPlan(symbolAllocator, topNA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, topNB.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        // validate no adaptation is required
        assertThat(topNA.adaptCommonSubplan(topNA.getCommonSubplan(), idAllocator)).isEqualTo(topNA.getCommonSubplan());
        assertThat(topNB.adaptCommonSubplan(topNB.getCommonSubplan(), idAllocator)).isEqualTo(topNB.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(createVarcharType(25), BIGINT);
        assertThat(topNA.getCommonSubplanSignature()).isEqualTo(topNB.getCommonSubplanSignature());
        assertThat(topNA.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNRankingKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableList.of(),
                                ImmutableMap.of(REGIONKEY_ID, ASC_NULLS_LAST),
                                RANK, 6),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        NATIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testCacheTopNRankingRankWithNonPullableConjuncts()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        (SELECT name, regionkey FROM nation WHERE nationkey > 10 ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES)
                        UNION ALL
                        (SELECT name, regionkey FROM nation WHERE nationkey > 11 ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES)
                        """,
                false, true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(TopNRankingNode.class));
        CommonPlanAdaptation topNA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation topNB = Iterables.get(planAdaptations.values(), 1);
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();

        PlanMatchPattern commonSubplanA = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of(),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", ASC_NULLS_LAST))
                        .rankingType(RANK)
                        .maxRankingPerPartition(6)
                        .partial(true),
                strictProject(ImmutableMap.of(
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY"))),
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "10")),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")))));
        assertTpchPlan(symbolAllocator, topNA.getCommonSubplan(), commonSubplanA);
        PlanMatchPattern commonSubplanB = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of(),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", ASC_NULLS_LAST))
                        .rankingType(RANK)
                        .maxRankingPerPartition(6)
                        .partial(true),
                strictProject(ImmutableMap.of(
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY"))),
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "11")),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")))));
        assertTpchPlan(symbolAllocator, topNB.getCommonSubplan(), commonSubplanB);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        // validate no adaptation is required
        assertThat(topNA.adaptCommonSubplan(topNA.getCommonSubplan(), idAllocator)).isEqualTo(topNA.getCommonSubplan());
        assertThat(topNB.adaptCommonSubplan(topNB.getCommonSubplan(), idAllocator)).isEqualTo(topNB.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(createVarcharType(25), BIGINT);
        assertThat(topNA.getCommonSubplanSignature()).isNotEqualTo(topNB.getCommonSubplanSignature());
        assertThat(topNA.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNRankingKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableList.of(),
                                ImmutableMap.of(REGIONKEY_ID, ASC_NULLS_LAST),
                                RANK, 6),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        NATIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testCacheTopNRankingRow()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        SELECT *
                        FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY name, nationkey ORDER BY regionkey DESC) update_rank FROM nation) AS t
                        WHERE t.update_rank = 1""",
                false, true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(TopNRankingNode.class));
        CommonPlanAdaptation topNRanking = planAdaptations.values().stream().findFirst().get();
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        PlanMatchPattern commonSubplan = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of("NAME", "NATIONKEY"),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", DESC_NULLS_LAST))
                        .rankingType(ROW_NUMBER)
                        .maxRankingPerPartition(1)
                        .partial(true),
                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")));
        assertTpchPlan(symbolAllocator, topNRanking.getCommonSubplan(), commonSubplan);
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();

        // validate no adaptation is required
        assertThat(topNRanking.adaptCommonSubplan(topNRanking.getCommonSubplan(), idAllocator)).isEqualTo(topNRanking.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25), BIGINT);
        assertThat(topNRanking.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNRankingKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableList.of(NAME_ID, NATIONKEY_ID),
                                ImmutableMap.of(REGIONKEY_ID, DESC_NULLS_LAST),
                                ROW_NUMBER, 1),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testCacheTopNRankingRowUnionWithSwappedPartitionBy()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        (SELECT *
                        FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY nationkey, name ORDER BY regionkey DESC) update_rank
                        FROM nation WHERE regionkey < 10) AS t
                        WHERE t.update_rank = 1)
                        UNION ALL (SELECT *
                        FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY name, nationkey ORDER BY regionkey DESC) update_rank
                        FROM nation WHERE regionkey < 10) AS t
                        WHERE t.update_rank = 1)""",
                false, true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(TopNRankingNode.class));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        CommonPlanAdaptation topNA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation topNB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of("NAME", "NATIONKEY"),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", DESC_NULLS_LAST))
                        .rankingType(ROW_NUMBER)
                        .maxRankingPerPartition(1)
                        .partial(true),
                filter(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey"))));
        assertTpchPlan(symbolAllocator, topNA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, topNB.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertTpchPlan(symbolAllocator, topNA.adaptCommonSubplan(topNA.getCommonSubplan(), idAllocator), commonSubplan);
        assertTpchPlan(symbolAllocator, topNB.adaptCommonSubplan(topNB.getCommonSubplan(), idAllocator), commonSubplan);
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25), BIGINT);
        assertThat(topNA.getCommonSubplanSignature()).isEqualTo(topNB.getCommonSubplanSignature());
        assertThat(topNA.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNRankingKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableList.of(NAME_ID, NATIONKEY_ID),
                                ImmutableMap.of(REGIONKEY_ID, DESC_NULLS_LAST),
                                ROW_NUMBER, 1),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testTopNRankingRowWithWithNonPullableConjuncts()
    {
        @Language("SQL") String query = """
                (SELECT *
                FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY nationkey ORDER BY regionkey DESC) update_rank
                FROM nation WHERE regionkey < 11) AS t
                WHERE t.update_rank = 1)
                UNION ALL (SELECT *
                FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY nationkey ORDER BY regionkey DESC) update_rank
                FROM nation WHERE regionkey < 10) AS t
                WHERE t.update_rank = 1)""";
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries(query, true, true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        // common subplans has higher priority than aggregations
        assertThat(planAdaptations).noneSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(TopNRankingNode.class));
        commonSubqueries = extractTpchCommonSubqueries(query, false, true, false, false);
        planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(TopNRankingNode.class));
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        CommonPlanAdaptation topNRankingA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation topNRankingB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplanA = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of("NATIONKEY"),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", DESC_NULLS_LAST))
                        .rankingType(ROW_NUMBER)
                        .maxRankingPerPartition(1)
                        .partial(true),
                filter(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey"))));
        PlanMatchPattern commonSubplanB = topNRanking(pattern -> pattern.specification(
                                ImmutableList.of("NATIONKEY"),
                                ImmutableList.of("REGIONKEY"),
                                ImmutableMap.of("REGIONKEY", DESC_NULLS_LAST))
                        .rankingType(ROW_NUMBER)
                        .maxRankingPerPartition(1)
                        .partial(true),
                filter(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "11")),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey"))));
        assertTpchPlan(symbolAllocator, topNRankingB.getCommonSubplan(), commonSubplanA);
        assertTpchPlan(symbolAllocator, topNRankingA.getCommonSubplan(), commonSubplanB);
        assertThat(topNRankingA.getCommonSubplanSignature()).isNotEqualTo(topNRankingB.getCommonSubplanSignature());
    }

    @Test
    public void testCacheTopN()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        SELECT nationkey FROM nation
                        WHERE regionkey > 10 and nationkey > 2
                        ORDER BY name ASC, regionkey DESC OFFSET 5 LIMIT 5""",
                false, true, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(TopNNode.class));
        CommonPlanAdaptation topN = planAdaptations.values().stream().findFirst().get();
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        PlanMatchPattern commonSubplan = topN(
                10,
                ImmutableList.of(sort("NAME", Ordering.ASCENDING, NullOrdering.LAST),
                        sort("REGIONKEY", Ordering.DESCENDING, NullOrdering.LAST)),
                TopNNode.Step.PARTIAL,
                filter(
                        new LogicalExpression(AND, ImmutableList.of(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")),
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "2")))),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey"))));
        assertTpchPlan(symbolAllocator, topN.getCommonSubplan(), commonSubplan);

        // validate no adaptation is required
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(topN.adaptCommonSubplan(topN.getCommonSubplan(), idAllocator)).isEqualTo(topN.getCommonSubplan());

        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25), BIGINT);
        assertThat(topN.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableMap.of(NAME_ID, ASC_NULLS_LAST, REGIONKEY_ID, DESC_NULLS_LAST), 10),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false),
                        NATIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 2L)), false)))));
    }

    @Test
    public void testTopNWithNonPullableConjuncts()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        (SELECT nationkey FROM nation WHERE regionkey > 1 ORDER BY name ASC OFFSET 5 LIMIT 5)
                        UNION ALL
                        (SELECT regionkey FROM nation WHERE regionkey > 2 ORDER BY name ASC OFFSET 5 LIMIT 5)""",
                true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).noneSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(TopNNode.class));
    }

    @Test
    public void testMultipleCacheTopN()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        (SELECT nationkey FROM nation WHERE regionkey < 10 ORDER BY name ASC OFFSET 5 LIMIT 5)
                        UNION ALL
                        (SELECT regionkey FROM nation WHERE regionkey < 10 ORDER BY name ASC OFFSET 5 LIMIT 5)""",
                true, false, false);
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(TopNNode.class));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        CommonPlanAdaptation topNA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation topNB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = topN(
                10,
                ImmutableList.of(sort("NAME", Ordering.ASCENDING, NullOrdering.LAST)),
                TopNNode.Step.PARTIAL,
                filter(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey"))));
        assertTpchPlan(symbolAllocator, topNA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, topNB.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertTpchPlan(symbolAllocator, topNA.adaptCommonSubplan(topNA.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of(
                                "NATIONKEY", PlanMatchPattern.expression(new SymbolReference("NATIONKEY")),
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME"))),
                        commonSubplan));
        assertTpchPlan(symbolAllocator, topNB.adaptCommonSubplan(topNB.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of(
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY")),
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME"))),
                        commonSubplan));
        assertThat(topNA.getCommonSubplanSignature()).isEqualTo(topNB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25), BIGINT);
        assertThat(topNB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        topNKey(
                                scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                                ImmutableMap.of(NAME_ID, ASC_NULLS_LAST), 10),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testCacheSingleAggregation()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        SELECT sum(nationkey) FROM nation
                        WHERE regionkey > 10
                        GROUP BY name""",
                true, true, true);

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(AggregationNode.class));

        CommonPlanAdaptation aggregation = Iterables.get(planAdaptations.values(), 0);
        PlanMatchPattern commonSubplan = aggregation(
                singleGroupingSet("NAME"),
                ImmutableMap.of(
                        Optional.of("SUM"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY")))),
                Optional.empty(),
                AggregationNode.Step.PARTIAL,
                identityProject(
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey")))));

        // validate common subplan
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, aggregation.getCommonSubplan(), commonSubplan);

        // validate no adaptation is required
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(aggregation.adaptCommonSubplan(aggregation.getCommonSubplan(), idAllocator)).isEqualTo(aggregation.getCommonSubplan());

        // validate signature
        Expression sum = canonicalAggregation("sum", NATIONKEY_EXPRESSION);
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NAME_ID, canonicalExpressionToColumnId(sum));
        RowType rowType = RowType.from(List.of(RowType.field(BIGINT), RowType.field(BIGINT)));
        List<Type> cacheColumnsTypes = ImmutableList.of(createVarcharType(25), rowType);
        assertThat(aggregation.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of(NAME_ID)),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testCacheSingleProjection()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                        SELECT sum(nationkey) FROM nation
                        WHERE regionkey > 10
                        GROUP BY name""",
                true, false, true);

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(1);
        assertThat(planAdaptations).allSatisfy((node, adaptation) -> assertThat(node).isInstanceOf(ProjectNode.class));

        CommonPlanAdaptation projection = Iterables.get(planAdaptations.values(), 0);
        PlanMatchPattern commonSubplan =
                identityProject(
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey"))));

        // validate common subplan
        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, projection.getCommonSubplan(), commonSubplan);

        // validate no adaptation is required
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(projection.adaptCommonSubplan(projection.getCommonSubplan(), idAllocator)).isEqualTo(projection.getCommonSubplan());

        // validate signature
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, NAME_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25));
        assertThat(projection.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testSimpleAggregation()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT sum(nationkey) FROM nation
                UNION ALL
                SELECT sum(nationkey) FROM nation""");
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(AggregationNode.class));

        CommonPlanAdaptation aggregationA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation aggregationB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = aggregation(
                globalAggregation(),
                ImmutableMap.of(Optional.of("SUM"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY")))),
                Optional.empty(),
                AggregationNode.Step.PARTIAL,
                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, aggregationB.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(aggregationA.adaptCommonSubplan(aggregationA.getCommonSubplan(), idAllocator)).isEqualTo(aggregationA.getCommonSubplan());
        assertThat(aggregationB.adaptCommonSubplan(aggregationB.getCommonSubplan(), idAllocator)).isEqualTo(aggregationB.getCommonSubplan());

        // make sure plan signatures are same
        Expression sum = canonicalAggregation("sum", NATIONKEY_EXPRESSION);
        assertThat(aggregationA.getCommonSubplanSignature()).isEqualTo(aggregationB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(canonicalExpressionToColumnId(sum));
        RowType rowType = RowType.from(List.of(RowType.field(BIGINT), RowType.field(BIGINT)));
        List<Type> cacheColumnsTypes = ImmutableList.of(rowType);
        assertThat(aggregationB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of()),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testGlobalAggregation()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT * FROM
                (SELECT sum(nationkey), max(regionkey) FILTER(WHERE nationkey > 10) FROM nation)
                CROSS JOIN
                (SELECT
                  sum(nationkey),
                  avg(nationkey * 2) FILTER(WHERE nationkey > 10)
                FROM nation)""");
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(AggregationNode.class));

        CommonPlanAdaptation aggregationA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation aggregationB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = aggregation(
                globalAggregation(),
                ImmutableMap.of(
                        Optional.of("MAX_FILTERED"), aggregationFunction("max", false, ImmutableList.of(symbol("REGIONKEY"))),
                        Optional.of("SUM"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY"))),
                        Optional.of("AVG_FILTERED"), aggregationFunction("avg", false, ImmutableList.of(symbol("MULTIPLICATION")))),
                ImmutableList.of(),
                ImmutableList.of("MASK"),
                Optional.empty(),
                AggregationNode.Step.PARTIAL,
                project(ImmutableMap.of(
                                "MULTIPLICATION", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "2"))),
                                "MASK", PlanMatchPattern.expression(new ComparisonExpression(GREATER_THAN, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "10")))),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey"))));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, aggregationB.getCommonSubplan(), commonSubplan);
        assertAggregationsWithMasks(aggregationA.getCommonSubplan(), 0, 2);
        assertAggregationsWithMasks(aggregationB.getCommonSubplan(), 0, 2);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.adaptCommonSubplan(aggregationA.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of(
                                "SUM", PlanMatchPattern.expression(new SymbolReference("SUM")),
                                "MAX_FILTERED", PlanMatchPattern.expression(new SymbolReference("MAX_FILTERED"))),
                        commonSubplan));
        assertTpchPlan(symbolAllocator, aggregationB.adaptCommonSubplan(aggregationB.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of(
                                "SUM", PlanMatchPattern.expression(new SymbolReference("SUM")),
                                "AVG_FILTERED", PlanMatchPattern.expression(new SymbolReference("AVG_FILTERED"))),
                        commonSubplan));

        // make sure plan signatures are same
        CacheColumnId nationKeyGreaterThan10 = canonicalExpressionToColumnId(new ComparisonExpression(GREATER_THAN, new SymbolReference("[nationkey:bigint]"), new GenericLiteral("BIGINT", "10")));
        CacheColumnId nationKeyMultiplyBy2 = canonicalExpressionToColumnId(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("[nationkey:bigint]"), new GenericLiteral("BIGINT", "2")));
        Expression max = canonicalAggregation(
                "max",
                Optional.of(columnIdToSymbol(nationKeyGreaterThan10)),
                new ExpressionWithType(new SymbolReference("[regionkey:bigint]"), BIGINT));
        Expression sum = canonicalAggregation("sum", NATIONKEY_EXPRESSION);
        Expression avg = canonicalAggregation(
                "avg",
                Optional.of(columnIdToSymbol(nationKeyGreaterThan10)),
                new ExpressionWithType(nationKeyMultiplyBy2, BIGINT));
        assertThat(aggregationA.getCommonSubplanSignature()).isEqualTo(aggregationB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(canonicalExpressionToColumnId(max), canonicalExpressionToColumnId(sum), canonicalExpressionToColumnId(avg));
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT,
                RowType.from(List.of(RowType.field(BIGINT), RowType.field(BIGINT))),
                RowType.from(List.of(RowType.field(DoubleType.DOUBLE), RowType.field(BIGINT))));
        //columnTypes=[bigint, row(bigint, bigint), row(double, bigint)],
        assertThat(aggregationB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of()),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testBigintGroupByColumnAggregation()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT sum(nationkey) FROM nation GROUP BY regionkey * 2
                UNION ALL
                SELECT sum(nationkey) FROM nation GROUP BY regionkey * 2
                UNION ALL
                SELECT nationkey FROM nation""");
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(AggregationNode.class));

        CommonPlanAdaptation aggregationA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation aggregationB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = aggregation(
                singleGroupingSet("MULTIPLICATION"),
                ImmutableMap.of(Optional.of("SUM"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY")))),
                Optional.empty(),
                AggregationNode.Step.PARTIAL,
                project(ImmutableMap.of(
                                "MULTIPLICATION", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "2")))),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey"))));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, aggregationB.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(aggregationA.adaptCommonSubplan(aggregationA.getCommonSubplan(), idAllocator)).isEqualTo(aggregationA.getCommonSubplan());
        assertThat(aggregationB.adaptCommonSubplan(aggregationB.getCommonSubplan(), idAllocator)).isEqualTo(aggregationB.getCommonSubplan());

        // make sure plan signatures are same
        CacheColumnId groupByColumn = canonicalExpressionToColumnId(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("[regionkey:bigint]"), new GenericLiteral("BIGINT", "2")));
        Expression sum = canonicalAggregation("sum", NATIONKEY_EXPRESSION);
        assertThat(aggregationA.getCommonSubplanSignature()).isEqualTo(aggregationB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(groupByColumn, canonicalExpressionToColumnId(sum));
        RowType rowType = RowType.from(List.of(RowType.field(BIGINT), RowType.field(BIGINT)));
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, rowType);
        assertThat(aggregationB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of(groupByColumn)),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testMultiColumnGroupByAggregation()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT sum(nationkey) FROM nation
                WHERE regionkey > 10 AND nationkey > 10
                GROUP BY regionkey, name
                UNION ALL
                SELECT max(nationkey)
                FROM nation
                WHERE regionkey < 5 AND nationkey > 10
                GROUP BY name, regionkey
                UNION ALL
                SELECT avg(nationkey) FROM nation
                WHERE regionkey > 10 AND nationkey > 11
                GROUP BY regionkey, name""");
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        // only aggregations with "nationkey > 10" predicate share common subqueries
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(AggregationNode.class));

        CommonPlanAdaptation aggregationA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation aggregationB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = aggregation(
                singleGroupingSet("REGIONKEY", "NAME"),
                ImmutableMap.of(
                        Optional.of("SUM"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY"))),
                        Optional.of("MAX"), aggregationFunction("max", false, ImmutableList.of(symbol("NATIONKEY")))),
                Optional.empty(),
                AggregationNode.Step.PARTIAL,
                filter(
                        new LogicalExpression(AND, ImmutableList.of(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "10")),
                                new LogicalExpression(OR, ImmutableList.of(
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")),
                                        new ComparisonExpression(LESS_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "5")))))),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey", "NAME", "name"))));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, aggregationB.getCommonSubplan(), commonSubplan);

        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.adaptCommonSubplan(aggregationA.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of(
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY")),
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "SUM", PlanMatchPattern.expression(new SymbolReference("SUM"))),
                        filter(new ComparisonExpression(GREATER_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "10")), commonSubplan)));
        assertTpchPlan(symbolAllocator, aggregationB.adaptCommonSubplan(aggregationB.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of(
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY")),
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "MAX", PlanMatchPattern.expression(new SymbolReference("MAX"))),
                        filter(new ComparisonExpression(LESS_THAN, new SymbolReference("REGIONKEY"), new GenericLiteral("BIGINT", "5")), commonSubplan)));

        // make sure plan signatures are same
        Expression sum = canonicalAggregation("sum", NATIONKEY_EXPRESSION);
        Expression max = canonicalAggregation("max", NATIONKEY_EXPRESSION);
        assertThat(aggregationA.getCommonSubplanSignature()).isEqualTo(aggregationB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(REGIONKEY_ID, NAME_ID, canonicalExpressionToColumnId(sum), canonicalExpressionToColumnId(max));
        RowType rowType = RowType.from(List.of(RowType.field(BIGINT), RowType.field(BIGINT)));
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, createVarcharType(25), rowType, BIGINT);
        assertThat(aggregationB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of(NAME_ID, REGIONKEY_ID)),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        REGIONKEY_ID, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 5L), greaterThan(BIGINT, 10L)), false),
                        NATIONKEY_ID, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 10L)), false)))));
    }

    @Test
    public void testAggregationWithComplexAggregationExpression()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT sum(nationkey + 1) FROM nation GROUP BY name, regionkey
                UNION ALL
                SELECT sum(nationkey + 1) FROM nation GROUP BY regionkey, name""");

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(AggregationNode.class));

        CommonPlanAdaptation aggregationA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation aggregationB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = aggregation(
                singleGroupingSet("NAME", "REGIONKEY"),
                ImmutableMap.of(
                        Optional.of("SUM"), aggregationFunction("sum", false, ImmutableList.of(symbol("EXPR")))),
                Optional.empty(),
                AggregationNode.Step.PARTIAL,
                strictProject(ImmutableMap.of(
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY")),
                                "EXPR", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "1")))),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "NAME", "name", "REGIONKEY", "regionkey"))));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, aggregationB.getCommonSubplan(), commonSubplan);

        // only subplan B required adaptation (different order for group by columns)
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertThat(aggregationA.adaptCommonSubplan(aggregationA.getCommonSubplan(), idAllocator)).isEqualTo(aggregationA.getCommonSubplan());
        assertTpchPlan(symbolAllocator, aggregationB.adaptCommonSubplan(aggregationB.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of(
                                "REGIONKEY", PlanMatchPattern.expression(new SymbolReference("REGIONKEY")),
                                "NAME", PlanMatchPattern.expression(new SymbolReference("NAME")),
                                "SUM", PlanMatchPattern.expression(new SymbolReference("SUM"))),
                        commonSubplan));

        // make sure plan signatures are same
        CacheColumnId nationKeyPlusOne = canonicalExpressionToColumnId(new ArithmeticBinaryExpression(ADD, new SymbolReference("[nationkey:bigint]"), new GenericLiteral("BIGINT", "1")));
        Expression sum = canonicalAggregation("sum", new ExpressionWithType(nationKeyPlusOne, BIGINT));
        assertThat(aggregationA.getCommonSubplanSignature()).isEqualTo(aggregationB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NAME_ID, REGIONKEY_ID, canonicalExpressionToColumnId(sum));
        RowType rowType = RowType.from(List.of(RowType.field(BIGINT), RowType.field(BIGINT)));
        List<Type> cacheColumnsTypes = ImmutableList.of(createVarcharType(25), BIGINT, rowType);
        assertThat(aggregationB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of(NAME_ID, REGIONKEY_ID)),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testNestedProjections()
    {
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT nationkey_mul * nationkey_mul FROM (SELECT nationkey * 2 AS nationkey_mul FROM nation)
                UNION ALL
                SELECT nationkey_add + nationkey_add FROM (SELECT nationkey + 2 AS nationkey_add FROM nation)""");

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(ProjectNode.class));

        CommonPlanAdaptation projectionA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation projectionB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = strictProject(
                ImmutableMap.of(
                        "MUL", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("NATIONKEY_MUL"), new SymbolReference("NATIONKEY_MUL"))),
                        "ADD", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("NATIONKEY_ADD"), new SymbolReference("NATIONKEY_ADD")))),
                strictProject(
                        ImmutableMap.of(
                                "NATIONKEY_MUL", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "2"))),
                                "NATIONKEY_ADD", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("NATIONKEY"), new GenericLiteral("BIGINT", "2")))),
                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey"))));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, projectionA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, projectionB.getCommonSubplan(), commonSubplan);

        // validate adaptations
        PlanNodeIdAllocator idAllocator = commonSubqueries.idAllocator();
        assertTpchPlan(symbolAllocator, projectionA.adaptCommonSubplan(projectionA.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of("MUL", PlanMatchPattern.expression(new SymbolReference("MUL"))),
                        commonSubplan));
        assertTpchPlan(symbolAllocator, projectionB.adaptCommonSubplan(projectionB.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of("ADD", PlanMatchPattern.expression(new SymbolReference("ADD"))),
                        commonSubplan));

        // make sure plan signatures are same
        SymbolReference nationKeyMultiplyReference = columnIdToSymbol(canonicalExpressionToColumnId(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("[nationkey:bigint]"), new GenericLiteral("BIGINT", "2")))).toSymbolReference();
        SymbolReference nationKeyAddReference = columnIdToSymbol(canonicalExpressionToColumnId(new ArithmeticBinaryExpression(ADD, new SymbolReference("[nationkey:bigint]"), new GenericLiteral("BIGINT", "2")))).toSymbolReference();
        Expression multiplyProjection = new ArithmeticBinaryExpression(MULTIPLY, nationKeyMultiplyReference, nationKeyMultiplyReference);
        Expression addProjection = new ArithmeticBinaryExpression(ADD, nationKeyAddReference, nationKeyAddReference);
        assertThat(projectionA.getCommonSubplanSignature()).isEqualTo(projectionB.getCommonSubplanSignature());
        assertThat(projectionB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        filterProjectKey(scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01"))),
                        Optional.empty(),
                        ImmutableList.of(canonicalExpressionToColumnId(multiplyProjection), canonicalExpressionToColumnId(addProjection)),
                        ImmutableList.of(BIGINT, BIGINT)),
                TupleDomain.all()));
    }

    @Test
    public void testQueryWithAggregatedAndNonAggregatedSubqueries()
    {
        // data should be cached on table scan level
        CommonSubqueries commonSubqueries = extractTpchCommonSubqueries("""
                SELECT sum(nationkey) FROM nation GROUP BY regionkey
                UNION ALL
                SELECT nationkey FROM nation""");

        Map<PlanNode, CommonPlanAdaptation> planAdaptations = commonSubqueries.planAdaptations();
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).allSatisfy((node, adaptation) ->
                assertThat(node).isInstanceOf(TableScanNode.class));

        CommonPlanAdaptation aggregationA = Iterables.get(planAdaptations.values(), 0);
        CommonPlanAdaptation aggregationB = Iterables.get(planAdaptations.values(), 1);

        PlanMatchPattern commonSubplan = tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey"));

        SymbolAllocator symbolAllocator = commonSubqueries.symbolAllocator();
        assertTpchPlan(symbolAllocator, aggregationA.getCommonSubplan(), commonSubplan);
        assertTpchPlan(symbolAllocator, aggregationB.getCommonSubplan(), commonSubplan);

        // make sure plan signatures are same
        assertThat(aggregationA.getCommonSubplanSignature()).isEqualTo(aggregationB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_ID, REGIONKEY_ID);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, BIGINT);
        assertThat(aggregationB.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(tpchCatalogId + ":tiny:nation:0.01")),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testExtractCommonSubqueries()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        Symbol subqueryAColumn2 = symbolAllocator.newSymbol("subquery_a_column2", BIGINT);
        Symbol subqueryAProjection1 = symbolAllocator.newSymbol("subquery_a_projection1", BIGINT);
        // subquery A scans column1 and column2
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1, subqueryAColumn2),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1, subqueryAColumn2, HANDLE_2),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        // subquery A has complex predicate, but no DF
        FilterNode filterA = new FilterNode(
                new PlanNodeId("filterA"),
                scanA,
                new LogicalExpression(OR, ImmutableList.of(
                        new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("subquery_a_column1"), new LongLiteral("4")), new GenericLiteral("BIGINT", "0")),
                        new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("subquery_a_column2"), new LongLiteral("2")), new GenericLiteral("BIGINT", "0")))));
        ProjectNode projectA = new ProjectNode(
                new PlanNodeId("projectA"),
                filterA,
                Assignments.of(
                        subqueryAProjection1, new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("subquery_a_column1"), new LongLiteral("10")),
                        subqueryAColumn1, new SymbolReference("subquery_a_column1")));

        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        Symbol subqueryBProjection1 = symbolAllocator.newSymbol("subquery_b_projection1", BIGINT);
        // subquery B scans just column 1
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn1),
                ImmutableMap.of(subqueryBColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        // Subquery B predicate is subset of subquery A predicate. Subquery B has dynamic filter
        FilterNode filterB = new FilterNode(
                new PlanNodeId("filterB"),
                scanB,
                and(
                        new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("subquery_b_column1"), new LongLiteral("4")), new GenericLiteral("BIGINT", "0")),
                        createDynamicFilterExpression(
                                getPlanTester().getPlannerContext().getMetadata(),
                                new DynamicFilterId("subquery_b_dynamic_id"),
                                BIGINT,
                                new SymbolReference("subquery_b_column1"))));
        // Subquery B projection is subset of subquery 1 projection
        ProjectNode projectB = new ProjectNode(
                new PlanNodeId("projectB"),
                filterB,
                Assignments.of(
                        subqueryBProjection1, new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("subquery_b_column1"), new LongLiteral("10"))));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(projectA, projectB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(projectA);
        assertThat(planAdaptations).containsKey(projectB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(projectA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(projectB);

        // common subplan should be identical for both subqueries
        PlanMatchPattern commonSubplanTableScan = strictTableScan(
                TEST_TABLE,
                ImmutableMap.of(
                        "column1", "column1",
                        "column2", "column2"));
        PlanMatchPattern commonSubplan = strictProject(
                ImmutableMap.of(
                        "column1", PlanMatchPattern.expression(new SymbolReference("column1")),
                        "projection", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("column1"), new LongLiteral("10")))),
                filter(
                        new LogicalExpression(OR, ImmutableList.of(
                                new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("column1"), new LongLiteral("4")), new GenericLiteral("BIGINT", "0")),
                                new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("column2"), new LongLiteral("2")), new GenericLiteral("BIGINT", "0")))),
                        commonSubplanTableScan));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // assert that FilteredTableScan has correct table and predicate for both subplans
        assertPlan(symbolAllocator, subqueryA.getCommonSubplanFilteredTableScan().tableScanNode(), commonSubplanTableScan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplanFilteredTableScan().tableScanNode(), commonSubplanTableScan);
        assertThat(subqueryA.getCommonSubplanFilteredTableScan().filterPredicate()).hasValue(
                ((FilterNode) PlanNodeSearcher.searchFrom(subqueryA.getCommonSubplan())
                        .whereIsInstanceOfAny(FilterNode.class)
                        .findOnlyElement())
                        .getPredicate());
        assertThat(subqueryB.getCommonSubplanFilteredTableScan().filterPredicate()).hasValue(
                ((FilterNode) PlanNodeSearcher.searchFrom(subqueryB.getCommonSubplan())
                        .whereIsInstanceOfAny(FilterNode.class)
                        .findOnlyElement())
                        .getPredicate());

        // assert that useConnectorNodePartitioning is propagated correctly
        assertThat(((TableScanNode) PlanNodeSearcher.searchFrom(subqueryA.getCommonSubplan())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findOnlyElement())
                .isUseConnectorNodePartitioning())
                .isFalse();
        assertThat(((TableScanNode) PlanNodeSearcher.searchFrom(subqueryB.getCommonSubplan())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findOnlyElement())
                .isUseConnectorNodePartitioning())
                .isFalse();

        // assert that common subplan for subquery A doesn't have dynamic filter
        assertThat(extractExpressions(subqueryA.getCommonSubplan()).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream()))
                .isEmpty();

        CacheColumnId column1 = new CacheColumnId("[cache_column1]");
        CacheColumnId column2 = new CacheColumnId("[cache_column2]");
        assertThat(subqueryA.getCommonColumnHandles()).containsExactly(
                entry(column1, HANDLE_1),
                entry(column2, HANDLE_2));

        // assert that common subplan for subquery B has dynamic filter preserved
        assertThat(extractExpressions(subqueryB.getCommonSubplan()).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .collect(toImmutableList()))
                .containsExactly(new Descriptor(
                        new DynamicFilterId("subquery_b_dynamic_id"),
                        new SymbolReference("subquery_b_column1")));
        assertThat(subqueryB.getCommonColumnHandles()).isEqualTo(subqueryA.getCommonColumnHandles());

        // common DF is true since subqueryA doesn't have DF
        assertThat(subqueryA.getCommonDynamicFilterDisjuncts()).isEqualTo(TRUE_LITERAL);
        assertThat(subqueryB.getCommonDynamicFilterDisjuncts()).isEqualTo(TRUE_LITERAL);

        // symbols used in common subplans for both subqueries should be unique
        assertThat(SymbolsExtractor.extractUnique(subqueryA.getCommonSubplan()))
                .doesNotContainAnyElementsOf(SymbolsExtractor.extractUnique(subqueryB.getCommonSubplan()));

        // since subqueryA has the same predicate and projections as common subquery, then no adaptation is required
        PlanNode subqueryACommonSubplan = subqueryA.getCommonSubplan();
        assertThat(subqueryA.adaptCommonSubplan(subqueryACommonSubplan, idAllocator)).isEqualTo(subqueryACommonSubplan);

        assertPlan(symbolAllocator, subqueryB.adaptCommonSubplan(subqueryB.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of("projection", PlanMatchPattern.expression(new SymbolReference("projection"))),
                        filter(
                                new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("column1"), new LongLiteral("4")), new GenericLiteral("BIGINT", "0")),
                                commonSubplan)));

        // make sure plan signatures are same
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(subqueryB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(canonicalExpressionToColumnId(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("[cache_column1]"), new LongLiteral("10"))), column1);
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, BIGINT);
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(new PlanSignature(
                combine(scanFilterProjectKey(new CacheTableId(testTableHandle.getCatalogHandle().getId() + ":cache_table_id")), "filters=((([cache_column1] % 4) = BIGINT '0') OR (([cache_column2] % 2) = BIGINT '0'))"),
                Optional.empty(),
                cacheColumnIds,
                cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testCommonPredicateWasPushedDownAndDynamicFilter()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), getPlanTester().getPlannerContext(), TEST_SESSION);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // subquery A
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);

        PlanNode planA = planBuilder.filter(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("subquery_a_column1"), new GenericLiteral("BIGINT", "150")),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryAColumn1))
                                .setAssignments(ImmutableMap.of(subqueryAColumn1, HANDLE_1))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // subquery B
        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        Symbol subqueryBColumn2 = symbolAllocator.newSymbol("subquery_b_column2", BIGINT);

        PlanNode planB = planBuilder.filter(
                and(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("subquery_b_column1"), new GenericLiteral("BIGINT", "50")),
                        createDynamicFilterExpression(
                                getPlanTester().getPlannerContext().getMetadata(),
                                new DynamicFilterId("subquery_b_dynamic_id"),
                                BIGINT,
                                new SymbolReference("subquery_b_column2"))),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryBColumn1, subqueryBColumn2))
                                .setAssignments(ImmutableMap.of(subqueryBColumn1, HANDLE_1, subqueryBColumn2, HANDLE_2))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // create a plan
        PlanNode root = planBuilder.union(ImmutableListMultimap.of(), ImmutableList.of(planA, planB));

        // extract common subqueries
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(idAllocator, symbolAllocator, root);
        CommonPlanAdaptation subqueryA = planAdaptations.get(planA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(planB);
        PlanMatchPattern commonTableScan = tableScan(TEST_TABLE, ImmutableMap.of("column2", "column2"))
                .with(TableScanNode.class, tableScan -> ((MockConnectorTableHandle) tableScan.getTable().getConnectorHandle()).getConstraint().equals(CONSTRAINT_1));

        // check whether common predicates were pushed down to common table scan
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonTableScan);

        // There is a FilterNode because of dynamic filters
        PlanMatchPattern commonSubplanB = filter(TRUE_LITERAL, createDynamicFilterExpression(
                        getPlanTester().getPlannerContext().getMetadata(),
                        new DynamicFilterId("subquery_b_dynamic_id"),
                        BIGINT,
                        new SymbolReference("column2")),
                commonTableScan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplanB);
    }

    @Test
    public void testCommonPredicateWasFullyPushedDown()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), getPlanTester().getPlannerContext(), TEST_SESSION);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // subquery A
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);

        PlanNode planA = planBuilder.filter(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("subquery_a_column1"), new GenericLiteral("BIGINT", "150")),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryAColumn1))
                                .setAssignments(ImmutableMap.of(subqueryAColumn1, HANDLE_1))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // subquery B
        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);

        PlanNode planB = planBuilder.filter(
                new ComparisonExpression(LESS_THAN, new SymbolReference("subquery_b_column1"), new GenericLiteral("BIGINT", "50")),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryBColumn1))
                                .setAssignments(ImmutableMap.of(subqueryBColumn1, HANDLE_1))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // create a plan
        PlanNode root = planBuilder.union(ImmutableListMultimap.of(), ImmutableList.of(planA, planB));

        // extract common subqueries
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(idAllocator, symbolAllocator, root);
        CommonPlanAdaptation subqueryA = planAdaptations.get(planA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(planB);

        // check whether common predicates were pushed down to common table scan
        PlanMatchPattern commonSubplan = tableScan(TEST_TABLE)
                .with(TableScanNode.class, tableScan -> ((MockConnectorTableHandle) tableScan.getTable().getConnectorHandle()).getConstraint().equals(CONSTRAINT_1));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);
    }

    @Test
    public void testCommonPredicateWasPartiallyPushedDown()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), getPlanTester().getPlannerContext(), TEST_SESSION);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // subquery A
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);

        PlanNode planA = planBuilder.filter(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("subquery_a_column1"), new GenericLiteral("BIGINT", "70")),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryAColumn1))
                                .setAssignments(ImmutableMap.of(subqueryAColumn1, HANDLE_1))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // subquery B
        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);

        PlanNode planB = planBuilder.filter(
                new ComparisonExpression(LESS_THAN, new SymbolReference("subquery_b_column1"), new GenericLiteral("BIGINT", "30")),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryBColumn1))
                                .setAssignments(ImmutableMap.of(subqueryBColumn1, HANDLE_1))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // create a plan
        PlanNode root = planBuilder.union(ImmutableListMultimap.of(), ImmutableList.of(planA, planB));

        // extract common subqueries
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(idAllocator, symbolAllocator, root);
        CommonPlanAdaptation subqueryA = planAdaptations.get(planA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(planB);

        // check whether common predicates were partially pushed down (there is remaining filter and pushed down filter to table handle)
        // to common table scan
        PlanMatchPattern commonSubplan = filter(
                new LogicalExpression(OR, ImmutableList.of(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "30")),
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "70")))),
                tableScan(TEST_TABLE, ImmutableMap.of("column1", "column1"))
                        .with(TableScanNode.class, tableScan -> ((MockConnectorTableHandle) tableScan.getTable().getConnectorHandle()).getConstraint().equals(CONSTRAINT_3)));

        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);
    }

    @Test
    public void testCommonPredicateWasNotPushedDownWhenValuesNode()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), getPlanTester().getPlannerContext(), TEST_SESSION);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // subquery A
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);

        PlanNode planA = planBuilder.filter(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("subquery_a_column1"), new GenericLiteral("BIGINT", "40")),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryAColumn1))
                                .setAssignments(ImmutableMap.of(subqueryAColumn1, HANDLE_1))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // subquery B
        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);

        PlanNode planB = planBuilder.filter(
                new ComparisonExpression(LESS_THAN, new SymbolReference("subquery_b_column1"), new GenericLiteral("BIGINT", "20")),
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setTableHandle(testTableHandle)
                                .setSymbols(ImmutableList.of(subqueryBColumn1))
                                .setAssignments(ImmutableMap.of(subqueryBColumn1, HANDLE_1))
                                .setEnforcedConstraint(TupleDomain.all())
                                .setUseConnectorNodePartitioning(Optional.of(false))));

        // create a plan
        PlanNode root = planBuilder.union(ImmutableListMultimap.of(), ImmutableList.of(planA, planB));

        // extract common subqueries
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(idAllocator, symbolAllocator, root);
        CommonPlanAdaptation subqueryA = planAdaptations.get(planA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(planB);

        PlanMatchPattern commonSubplan = filter(
                new LogicalExpression(OR, ImmutableList.of(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "20")),
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "40")))),
                tableScan(TEST_TABLE, ImmutableMap.of("column1", "column1"))
                        .with(TableScanNode.class, tableScan -> ((MockConnectorTableHandle) tableScan.getTable().getConnectorHandle()).getConstraint().equals(TupleDomain.all())));

        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);
    }

    @Test
    public void testExtractDomain()
    {
        // both subqueries contain simple predicate that can be translated into tuple domain in plan signature
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        FilterNode filterA = new FilterNode(
                new PlanNodeId("filterA"),
                scanA,
                new ComparisonExpression(GREATER_THAN, new SymbolReference("subquery_a_column1"), new GenericLiteral("BIGINT", "42")));

        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn1),
                ImmutableMap.of(subqueryBColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        FilterNode filterB = new FilterNode(
                new PlanNodeId("filterB"),
                scanB,
                new ComparisonExpression(LESS_THAN, new SymbolReference("subquery_b_column1"), new GenericLiteral("BIGINT", "0")));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(filterA, filterB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(filterA);
        assertThat(planAdaptations).containsKey(filterB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(filterA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(filterB);

        // common subplan should be identical for both subqueries
        PlanMatchPattern commonSubplan =
                filter(
                        new LogicalExpression(OR, ImmutableList.of(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "42")),
                                new ComparisonExpression(LESS_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "0")))),
                        strictTableScan(
                                TEST_TABLE,
                                ImmutableMap.of(
                                        "column1", "column1")));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // filtering adaptation is required
        assertPlan(symbolAllocator, subqueryA.adaptCommonSubplan(subqueryA.getCommonSubplan(), idAllocator),
                filter(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "42")),
                        commonSubplan));

        assertPlan(symbolAllocator, subqueryB.adaptCommonSubplan(subqueryB.getCommonSubplan(), idAllocator),
                filter(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "0")),
                        commonSubplan));

        // make sure plan signatures are same and contain domain
        SortedRangeSet expectedValues = (SortedRangeSet) ValueSet.ofRanges(lessThan(BIGINT, 0L), greaterThan(BIGINT, 42L));
        TupleDomain<CacheColumnId> expectedTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new CacheColumnId("[cache_column1]"), Domain.create(expectedValues, false)));
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(subqueryB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(new CacheColumnId("[cache_column1]"));
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT);
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(testTableHandle.getCatalogHandle().getId() + ":cache_table_id")),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                expectedTupleDomain));

        // make sure signature tuple domain is normalized
        SortedRangeSet actualValues = (SortedRangeSet) subqueryA.getCommonSubplanSignature()
                .predicate()
                .getDomains()
                .orElseThrow()
                .get(new CacheColumnId("[cache_column1]"))
                .getValues();
        assertBlockEquals(BIGINT, actualValues.getSortedRanges(), expectedValues.getSortedRanges());
        assertThat(actualValues.getSortedRanges()).isInstanceOf(LongArrayBlock.class);
    }

    @Test
    public void testSimpleSubqueries()
    {
        // both subqueries are just table scans
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));

        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        Symbol subqueryBColumn2 = symbolAllocator.newSymbol("subquery_b_column2", BIGINT);
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn2, subqueryBColumn1),
                ImmutableMap.of(subqueryBColumn2, HANDLE_2, subqueryBColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(scanA, scanB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(scanA);
        assertThat(planAdaptations).containsKey(scanB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(scanA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(scanB);

        // common subplan should be identical for both subqueries
        PlanMatchPattern commonSubplan =
                strictTableScan(
                        TEST_TABLE,
                        ImmutableMap.of(
                                "column1", "column1",
                                "column2", "column2"));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // only projection adaptation is required
        assertPlan(symbolAllocator, subqueryA.adaptCommonSubplan(subqueryA.getCommonSubplan(), idAllocator),
                strictProject(ImmutableMap.of("column1", PlanMatchPattern.expression(new SymbolReference("column1"))),
                        commonSubplan));

        assertPlan(symbolAllocator, subqueryB.adaptCommonSubplan(subqueryB.getCommonSubplan(), idAllocator),
                // order of common subquery output needs to shuffled to match original query
                strictProject(ImmutableMap.of(
                                "column2", PlanMatchPattern.expression(new SymbolReference("column2")),
                                "column1", PlanMatchPattern.expression(new SymbolReference("column1"))),
                        commonSubplan));

        // make sure plan signatures are same and contain domain
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(subqueryB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(new CacheColumnId("[cache_column1]"), new CacheColumnId("[cache_column2]"));
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT, BIGINT);
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(testTableHandle.getCatalogHandle().getId() + ":cache_table_id")),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.all()));
    }

    @Test
    public void testPredicateInSingleSubquery()
    {
        // one subquery has filter, the other does not
        // common subquery shouldn't have any predicate
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        FilterNode filterA = new FilterNode(
                new PlanNodeId("filterA"),
                scanA,
                new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("subquery_a_column1"), new GenericLiteral("BIGINT", "4")), new GenericLiteral("BIGINT", "0")));

        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn1),
                ImmutableMap.of(subqueryBColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(filterA, scanB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(filterA);
        assertThat(planAdaptations).containsKey(scanB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(filterA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(scanB);

        // common subplan should consist on only table scan
        PlanMatchPattern commonSubplan = strictTableScan(
                TEST_TABLE,
                ImmutableMap.of("column1", "column1"));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // only filtering adaptation is required on subplan a
        assertPlan(symbolAllocator, subqueryA.adaptCommonSubplan(subqueryA.getCommonSubplan(), idAllocator),
                filter(
                        new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("column1"), new GenericLiteral("BIGINT", "4")), new GenericLiteral("BIGINT", "0")),
                        commonSubplan));

        assertPlan(symbolAllocator, subqueryB.adaptCommonSubplan(subqueryB.getCommonSubplan(), idAllocator), commonSubplan);
    }

    @Test
    public void testSharedConjunct()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        // subquery A scans column1 and column2
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        Symbol subqueryAColumn2 = symbolAllocator.newSymbol("subquery_a_column2", BIGINT);
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1, subqueryAColumn2),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1, subqueryAColumn2, HANDLE_2),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        // subquery A has predicate on both columns
        FilterNode filterA = new FilterNode(
                new PlanNodeId("filterA"),
                scanA,
                new LogicalExpression(AND, ImmutableList.of(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("subquery_a_column1"), new GenericLiteral("BIGINT", "42")),
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("subquery_a_column2"), new GenericLiteral("BIGINT", "24")))));
        ProjectNode projectA = new ProjectNode(
                new PlanNodeId("projectA"),
                filterA,
                Assignments.of(
                        subqueryAColumn2, new SymbolReference("subquery_a_column2")));

        // subquery B scans column1 and column2
        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        Symbol subqueryBColumn2 = symbolAllocator.newSymbol("subquery_b_column2", BIGINT);
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn1, subqueryBColumn2),
                ImmutableMap.of(subqueryBColumn1, HANDLE_1, subqueryBColumn2, HANDLE_2),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        // subquery B has predicate on column1 only
        FilterNode filterB = new FilterNode(
                new PlanNodeId("filterA"),
                scanB,
                new ComparisonExpression(LESS_THAN, new SymbolReference("subquery_b_column1"), new GenericLiteral("BIGINT", "42")));
        ProjectNode projectB = new ProjectNode(
                new PlanNodeId("projectA"),
                filterB,
                Assignments.of(subqueryBColumn2, new SymbolReference("subquery_b_column2")));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(projectA, projectB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(projectA);
        assertThat(planAdaptations).containsKey(projectB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(projectA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(projectB);

        // common subplan should be identical for both subqueries
        PlanMatchPattern commonSubplanTableScan = strictTableScan(
                TEST_TABLE,
                ImmutableMap.of(
                        "column1", "column1",
                        "column2", "column2"));
        PlanMatchPattern commonSubplan = strictProject(
                ImmutableMap.of(
                        "column2", PlanMatchPattern.expression(new SymbolReference("column2"))),
                filter(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("column1"), new GenericLiteral("BIGINT", "42")),
                        commonSubplanTableScan));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // subquery A should have predicate adaptation
        assertPlan(symbolAllocator, subqueryA.adaptCommonSubplan(subqueryA.getCommonSubplan(), idAllocator),
                filter(new ComparisonExpression(GREATER_THAN, new SymbolReference("column2"), new GenericLiteral("BIGINT", "24")), commonSubplan));

        PlanNode subqueryBCommonSubplan = subqueryB.getCommonSubplan();
        assertThat(subqueryB.adaptCommonSubplan(subqueryBCommonSubplan, idAllocator)).isEqualTo(subqueryBCommonSubplan);

        // make sure plan signatures are same
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(subqueryB.getCommonSubplanSignature());
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(new CacheColumnId("[cache_column2]"));
        List<Type> cacheColumnsTypes = ImmutableList.of(BIGINT);
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(testTableHandle.getCatalogHandle().getId() + ":cache_table_id")),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnsTypes),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new CacheColumnId("[cache_column1]"), Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 5L), lessThan(BIGINT, 42L)), false)))));
    }

    private CanonicalAggregation canonicalAggregation(String name, ExpressionWithType... arguments)
    {
        return canonicalAggregation(name, Optional.empty(), arguments);
    }

    private CanonicalAggregation canonicalAggregation(String name, Optional<Symbol> mask, ExpressionWithType... arguments)
    {
        ResolvedFunction resolvedFunction = getPlanTester().getPlannerContext().getMetadata().resolveBuiltinFunction(
                name,
                TypeSignatureProvider.fromTypes(Stream.of(arguments)
                        .map(ExpressionWithType::type)
                        .collect(toImmutableList())));
        return new CanonicalAggregation(
                resolvedFunction,
                mask,
                Stream.of(arguments)
                        .map(ExpressionWithType::expression)
                        .collect(toImmutableList()));
    }

    // workaround for https://github.com/google/error-prone/issues/2713
    @SuppressWarnings("unused")
    private record ExpressionWithType(Expression expression, Type type)
    {
        public ExpressionWithType(CacheColumnId columnId, Type type)
        {
            this(columnIdToSymbol(columnId).toSymbolReference(), type);
        }
    }

    private CommonSubqueries extractTpchCommonSubqueries(@Language("SQL") String query)
    {
        return extractTpchCommonSubqueries(query, true, false, false);
    }

    private CommonSubqueries extractTpchCommonSubqueries(@Language("SQL") String query, boolean cacheSubqueries, boolean cacheAggregations, boolean cacheProjections)
    {
        return extractTpchCommonSubqueries(query, cacheSubqueries, cacheAggregations, cacheProjections, true);
    }

    private CommonSubqueries extractTpchCommonSubqueries(@Language("SQL") String query, boolean cacheSubqueries, boolean cacheAggregations, boolean cacheProjections, boolean forceSingleNode)
    {
        Session tpchSession = Session.builder(TPCH_SESSION)
                .setSystemProperty(CACHE_COMMON_SUBQUERIES_ENABLED, Boolean.toString(cacheSubqueries))
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, Boolean.toString(cacheAggregations))
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, Boolean.toString(cacheProjections))
                .build();
        PlanTester planTester = getPlanTester();
        return planTester.inTransaction(tpchSession, session -> {
            Plan plan = planTester.createPlan(session, query, planTester.getPlanOptimizers(forceSingleNode), planTester.getAlternativeOptimizers(), OPTIMIZED_AND_VALIDATED, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getPlanTester().getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            SymbolAllocator symbolAllocator = new SymbolAllocator(plan.getTypes().allTypes());
            PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
            return new CommonSubqueries(
                    CommonSubqueriesExtractor.extractCommonSubqueries(
                            new CacheController(),
                            getPlanTester().getPlannerContext(),
                            session,
                            idAllocator,
                            symbolAllocator,
                            new RuleTester(getPlanTester()).getTypeAnalyzer(),
                            plan.getRoot()),
                    symbolAllocator,
                    idAllocator,
                    plan.getRoot());
        });
    }

    record CommonSubqueries(Map<PlanNode, CommonPlanAdaptation> planAdaptations, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, PlanNode plan) {}

    private Map<PlanNode, CommonPlanAdaptation> extractCommonSubqueries(
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            PlanNode root)
    {
        return getPlanTester().inTransaction(TEST_SESSION, session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getPlanTester().getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            return CommonSubqueriesExtractor.extractCommonSubqueries(
                    new CacheController(),
                    getPlanTester().getPlannerContext(),
                    session,
                    idAllocator,
                    symbolAllocator,
                    new RuleTester(getPlanTester()).getTypeAnalyzer(),
                    root);
        });
    }

    private void assertAggregationsWithMasks(PlanNode node, int... indexes)
    {
        // assert aggregations at given indexes are masked/unmasked
        assertThat(node).isInstanceOf(AggregationNode.class);
        AggregationNode aggregation = (AggregationNode) node;
        List<Aggregation> aggregations = ImmutableList.copyOf(aggregation.getAggregations().values());
        Set<Integer> maskedAggregations = Arrays.stream(indexes).boxed().collect(toImmutableSet());
        for (int i = 0; i < aggregations.size(); ++i) {
            if (maskedAggregations.contains(i)) {
                assertThat(aggregations.get(i).getMask()).isPresent();
            }
            else {
                assertThat(aggregations.get(i).getMask()).isEmpty();
            }
        }
    }

    private void assertPlan(SymbolAllocator symbolAllocator, PlanNode root, PlanMatchPattern expected)
    {
        assertPlan(TEST_SESSION, symbolAllocator, root, expected);
    }

    private void assertTpchPlan(SymbolAllocator symbolAllocator, PlanNode root, PlanMatchPattern expected)
    {
        assertPlan(TPCH_SESSION, symbolAllocator, root, expected);
    }

    private void assertPlan(Session customSession, SymbolAllocator symbolAllocator, PlanNode root, PlanMatchPattern expected)
    {
        getPlanTester().inTransaction(customSession, session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getPlanTester().getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            Plan plan = new Plan(root, symbolAllocator.getTypes(), StatsAndCosts.empty());
            PlanAssert.assertPlan(session, getPlanTester().getPlannerContext().getMetadata(), createTestingFunctionManager(), noopStatsCalculator(), plan, expected);
            return null;
        });
    }
}
